from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import time
import logging
from datetime import datetime, timedelta, timezone
import psycopg2
import psycopg2.extras
import json
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

def fetch_and_process_clickup_data():
    
    def convert_timestamp(timestamp_str):
        if timestamp_str is not None:
            timestamp = int(timestamp_str) / 1000 
            dt_utc = datetime.utcfromtimestamp(timestamp).replace(tzinfo=timezone.utc)
            dt_gmt4 = dt_utc.astimezone(timezone(timedelta(hours=-4)))
            return dt_gmt4.strftime("%Y-%m-%d %H:%M:%S")
        return None 
                    
    logging.info("Iniciando a coleta de dados do ClickUp")

    api_key = Variable.get("clickup_api_key")
    url = "URL_CLICKUP"
    params = {
        "archived": "false",
        "include_markdown_description": "true",
        "include_closed": "true",
        "page": 0
    }

    headers = {'Authorization': api_key}
    all_data = []
    attempt = 0
    max_attempts = 5
    backoff_factor = 1.5

    while True:
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code in range(200, 300):
                data = response.json()
                for task in data['tasks']:
                    task['date_created'] = convert_timestamp(task.get('date_created'))
                    task['date_closed'] = convert_timestamp(task.get('date_closed'))
                    task['date_updated'] = convert_timestamp(task.get('date_updated'))
                    task['date_done'] = convert_timestamp(task.get('date_done'))
                    task['due_date'] = convert_timestamp(task.get('due_date'))
                    task['start_date'] = convert_timestamp(task.get('start_date'))
                    
                all_data.extend(data['tasks'])

                logging.info(f"Processada página {params['page']}")

                if data.get('last_page', False):
                    break
                else:
                    params['page'] += 1
                    attempt = 0

            elif response.status_code == 429:
                logging.warning("Limite de taxa atingido, aguardando para tentar novamente")
                time.sleep((2 ** attempt) * backoff_factor)
                attempt += 1
                if attempt >= max_attempts:
                    logging.error("Número máximo de tentativas atingido para a página atual")
                    break

            else:
                logging.error(f"Erro ao buscar dados: {response.status_code}")
                break

        except requests.RequestException as e:
            logging.error(f"Erro na requisição: {e}")
            time.sleep((2 ** attempt) * backoff_factor)
            attempt += 1
            if attempt >= max_attempts:
                logging.error("Número máximo de tentativas atingido")
                break

    logging.info("Coleta de dados do ClickUp concluída")
    return all_data

def save_data_to_database(**kwargs):
    task_instance = kwargs['ti']
    all_data = task_instance.xcom_pull(task_ids='fetch_clickup_data')

    def batch_upsert_tasks(cursor, tasks):
        task_values = [
            (task["id"], task.get("name", ""), task.get("text_content", ""), 
             task.get("status", {}).get("status", ""), task.get("archived", False),
             task.get("team_id", None), task.get("url", ""),
             task.get("date_created", None), task.get("date_done", None), 
             task.get("date_closed", None), task.get("date_updated", None),
             task.get("due_date", None), task.get("start_date", None)) 
            for task in tasks
        ]
        psycopg2.extras.execute_batch(
            cursor,
            """
            INSERT INTO tec_task (id, name, text_content, status, archived, team_id, url, date_created, date_done, date_closed, date_updated, due_date, start_date) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET 
                name = EXCLUDED.name,
                text_content = EXCLUDED.text_content,
                status = EXCLUDED.status,
                archived = EXCLUDED.archived,
                team_id = EXCLUDED.team_id,
                url = EXCLUDED.url,
                date_created = EXCLUDED.date_created,
                date_done = EXCLUDED.date_done,
                date_closed = EXCLUDED.date_closed,
                date_updated = EXCLUDED.date_updated,
                due_date = EXCLUDED.due_date,
                start_date = EXCLUDED.start_date
            """,
            task_values
        )
        
    def batch_upsert_users(cursor, assignees):
        user_values = [
            (assignee["id"], assignee.get("username", ""), assignee.get("email", ""), assignee.get("profilePicture", "")) 
            for assignee in assignees
        ]
        psycopg2.extras.execute_batch(
            cursor,
            """
            INSERT INTO tec_users (id, username, email, profile_picture) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET 
                username = EXCLUDED.username,
                email = EXCLUDED.email,
                profile_picture = EXCLUDED.profile_picture
            """,
            user_values
        )
        
    def batch_upsert_task_assignees(cursor, task_assignee_data):
        psycopg2.extras.execute_batch(
            cursor,
            """
            INSERT INTO tec_task_assignees (task_id, user_id) 
            VALUES (%s, %s)
            ON CONFLICT (task_id, user_id) DO NOTHING
            """,
            task_assignee_data
        )
        
    def batch_upsert_custom_fields(cursor, tasks):
        logging.info("Iniciando upsert de campos personalizados.")
        custom_fields_data = []
        for task in tasks:
            task_id = task["id"]
            for custom_field in task.get("custom_fields", []):
                custom_field_id = custom_field["id"]
                custom_field_name = custom_field["name"]
                custom_field_value = custom_field.get("value", "")

                if isinstance(custom_field_value, (dict, list)):
                    custom_field_value = json.dumps(custom_field_value)

                unique_id = f"{task_id}_{custom_field_id}"

                custom_fields_data.append((unique_id, task_id, custom_field_id, custom_field_name, custom_field_value))

        if not custom_fields_data:
            logging.info("Nenhum dado de campo personalizado para fazer upsert.")
            return

        logging.info(f"Preparando para fazer upsert de {len(custom_fields_data)} campos personalizados.")

        try:
            psycopg2.extras.execute_batch(
                cursor,
                """
                INSERT INTO tec_custom_fields (id, task_id, custom_field_id, field_name, value)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                task_id = EXCLUDED.task_id,
                custom_field_id = EXCLUDED.custom_field_id,
                field_name = EXCLUDED.field_name,
                value = EXCLUDED.value
                """,
                custom_fields_data
            )
            logging.info("Upsert de campos personalizados concluído com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao fazer upsert de campos personalizados: {e}")
            
    current_task_ids = {data["id"] for data in all_data}

    try:
        conn_config = BaseHook.get_connection("clickup_db")
        conn = psycopg2.connect(
            dbname=conn_config.schema, 
            user=conn_config.login, 
            password=conn_config.password, 
            host=conn_config.host        
        )
        cursor = conn.cursor()
        
        batch_upsert_tasks(cursor, all_data)
        conn.commit()
        
        assignees = []
        task_assignee_data = []

        for data in all_data:
            for assignee in data.get("assignees", []):
                if isinstance(assignee, dict):
                    assignees.append(assignee)
                    task_assignee_data.append((data["id"], assignee["id"]))

        batch_upsert_users(cursor, assignees)
        batch_upsert_task_assignees(cursor, task_assignee_data)
        batch_upsert_custom_fields(cursor, all_data)  
        
        cursor.execute("SELECT id FROM task")
        db_task_ids = {row[0] for row in cursor.fetchall()}

        tasks_to_remove = db_task_ids - current_task_ids

        if tasks_to_remove:
            format_strings = ','.join(['%s'] * len(tasks_to_remove))
            cursor.execute("DELETE FROM task_assignees WHERE task_id IN (%s)" % format_strings,
                        tuple(tasks_to_remove))

        if tasks_to_remove:
            format_strings = ','.join(['%s'] * len(tasks_to_remove))
            cursor.execute("DELETE FROM task WHERE id IN (%s)" % format_strings,
                        tuple(tasks_to_remove))

        conn.commit()
    except psycopg2.DatabaseError as e:
        print(f"Erro no banco de dados: {e}")
    finally:
        if conn is not None:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clickup_task_passagem',
    default_args=default_args,
    description='A simple and performant DAG for processing ClickUp data',
    schedule_interval="0 * * * *",
    catchup=False
)

fetch_data_task = PythonOperator(
    task_id='fetch_clickup_data',
    python_callable=fetch_and_process_clickup_data,
    dag=dag,
)

save_data_task = PythonOperator(
    task_id='save_data_to_db',
    python_callable=save_data_to_database,
    provide_context=True,
    dag=dag,
)

fetch_data_task >> save_data_task
