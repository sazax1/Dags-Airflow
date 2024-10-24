import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
from airflow.hooks.base_hook import BaseHook
import psycopg2

# Função para transferência de dados dos bots
def transfer_data_bots():
    logging.info("Iniciando a transferência de dados dos bots...")

    # Conexão MongoDB
    try:
        logging.info("Conectando ao MongoDB...")
        mongo_conn = BaseHook.get_connection('mongo_default')
        mongo_client = MongoClient(f"mongodb+srv://{mongo_conn.login}:{mongo_conn.password}@{mongo_conn.host}/{mongo_conn.schema}")
        mongo_db = mongo_client[mongo_conn.schema]
        mongo_collection = mongo_db['bots']
        logging.info("Conexão com o MongoDB estabelecida com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao conectar ao MongoDB: {e}")
        return

    # Conexão PostgreSQL
    try:
        logging.info("Conectando ao PostgreSQL...")
        postgres_conn = BaseHook.get_connection('postgres_default')
        pg_conn = psycopg2.connect(dbname=postgres_conn.schema, user=postgres_conn.login, password=postgres_conn.password, host=postgres_conn.host)
        pg_cur = pg_conn.cursor()
        logging.info("Conexão com o PostgreSQL estabelecida com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao conectar ao PostgreSQL: {e}")
        return

    # Inserção no PostgreSQL
    try:
        bots = mongo_collection.find()
        for bot in bots:
            token = bot['token']
            name = bot['name']
            pg_cur.execute('INSERT INTO bots (token, name) VALUES (%s, %s) ON CONFLICT (token) DO NOTHING', (token, name))
        pg_conn.commit()
        logging.info("Dados dos bots inseridos no PostgreSQL com sucesso.")
    except Exception as e:
        logging.error(f"Erro durante a inserção de dados dos bots: {e}")
        pg_conn.rollback()
    finally:
        pg_cur.close()
        pg_conn.close()
        mongo_client.close()
        logging.info("Conexões fechadas.")

# Função para transferência de dados dos leads
def transfer_data_leads():
    logging.info("Iniciando a transferência de dados dos leads...")

    # Conexão MongoDB
    try:
        logging.info("Conectando ao MongoDB...")
        mongo_conn = BaseHook.get_connection('mongo_default')
        mongo_client = MongoClient(f"mongodb+srv://{mongo_conn.login}:{mongo_conn.password}@{mongo_conn.host}/{mongo_conn.schema}")
        mongo_db = mongo_client[mongo_conn.schema]
        mongo_collection = mongo_db['leads']
        logging.info("Conexão com o MongoDB estabelecida com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao conectar ao MongoDB: {e}")
        return

    # Agregação no MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {
                    "bot": "$bot",
                    "day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}
                },
                "member_count": {"$sum": {"$cond": [{"$eq": ["$channel.status", "member"]}, 1, 0]}},
                "left_count": {"$sum": {"$cond": [{"$eq": ["$channel.status", "left"]}, 1, 0]}},
                "empty_count": {"$sum": {"$cond": [{"$eq": ["$channel.status", ""]}, 1, 0]}},
                "other_status_count": {"$sum": {"$cond": [{"$eq": ["$channel.status", "other_status"]}, 1, 0]}}
            }
        }
    ]

    aggregation_results = mongo_collection.aggregate(pipeline)
    results = list(aggregation_results)
    logging.info(f"Agregação concluída com {len(results)} resultados.")

    # Conexão PostgreSQL
    try:
        logging.info("Conectando ao PostgreSQL...")
        postgres_conn = BaseHook.get_connection('postgres_default')
        pg_conn = psycopg2.connect(dbname=postgres_conn.schema, user=postgres_conn.login, password=postgres_conn.password, host=postgres_conn.host)
        pg_cur = pg_conn.cursor()
        logging.info("Conexão com o PostgreSQL estabelecida com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao conectar ao PostgreSQL: {e}")
        return

    # Inserção no PostgreSQL
    try:
        for result in results:
            bot = result["_id"]["bot"]
            day = datetime.strptime(result["_id"]["day"], '%Y-%m-%d').date()
            member_count = result["member_count"]
            left_count = result["left_count"]
            empty_count = result["empty_count"]
            other_status_count = result["other_status_count"]
            logging.debug(f"Inserindo dados: {result}")
            pg_cur.execute(
                '''INSERT INTO leads (bot, day, member_count, left_count, empty_count, other_status_count)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (bot, day) DO UPDATE SET
                   member_count = EXCLUDED.member_count,
                   left_count = EXCLUDED.left_count,
                   empty_count = EXCLUDED.empty_count,
                   other_status_count = EXCLUDED.other_status_count''',
                (bot, day, member_count, left_count, empty_count, other_status_count)
            )
        pg_conn.commit()
        logging.info("Dados agregados dos leads inseridos no PostgreSQL com sucesso.")
    except Exception as e:
        logging.error(f"Erro durante a inserção de dados agregados dos leads: {e}")
        pg_conn.rollback()
    finally:
        pg_cur.close()
        pg_conn.close()
        mongo_client.close()
        logging.info("Conexões fechadas.")

# Definir os argumentos padrões da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir a DAG
dag = DAG(
    'mongo_to_postgres_transfer',
    default_args=default_args,
    description='Transfer data from MongoDB to PostgreSQL every hour',
    schedule_interval=timedelta(hours=1),
)

# Definir as tarefas
transfer_data_bots_task = PythonOperator(
    task_id='transfer_data_bots',
    python_callable=transfer_data_bots,
    dag=dag,
)

transfer_data_leads_task = PythonOperator(
    task_id='transfer_data_leads',
    python_callable=transfer_data_leads,
    dag=dag,
)

# Configuração da DAG
transfer_data_bots_task >> transfer_data_leads_task