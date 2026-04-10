from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import mysql.connector
from mysql.connector import Error
import time

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

POSTGRES_CONFIG = {
    "host": "postgres",
    "database": "source",
    "user": "root",
    "password": "root"
}

MYSQL_CONFIG = {
    "host": "mysql",   
    "port": 3306,      
    "database": "destination_db",
    "user": "root",
    "password": "root"
}

LAST_SYNC_VAR = "last_sync_time"

def get_last_sync_time():
    """Retrieve last sync timestamp from Airflow Variables (or default to old date)."""
    value = Variable.get(LAST_SYNC_VAR, default_var=None)
    if value:
        return datetime.fromisoformat(value)
    return datetime(1970, 1, 1)

def extract_incremental(**context):
    last_sync = get_last_sync_time()
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    query = """
        SELECT * FROM car_sales
        WHERE last_update > %s
        ORDER BY last_update ASC;
    """
    df = pd.read_sql(query, conn, params=(last_sync,))
    conn.close()
    print(f"Extracted {len(df)} new/updated rows since {last_sync}")

    if 'last_update' in df.columns:
        df['last_update'] = df['last_update'].astype(str)

    return df.to_dict(orient="records")

def wait_for_mysql(**context):
    max_wait = 120  
    interval = 5   
    waited = 0
    while waited < max_wait:
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            conn.close()
            print("MySQL is ready!")
            return
        except Error:
            print("Waiting for MySQL...")
            time.sleep(interval)
            waited += interval
    raise Exception("MySQL not ready after 30 seconds")






def load_to_mysql(**context):
    records = context['ti'].xcom_pull(task_ids='extract_incremental')
    if not records:
        print("No new or updated records to load.")
        return

    wait_for_mysql()

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS car_sales (
            id INT PRIMARY KEY,
            car_model VARCHAR(100),
            seller_name VARCHAR(100),
            buyer_name VARCHAR(100),
            price DECIMAL(10,2),
            sale_date DATE,
            last_update TIMESTAMP
        )
    """)
    for row in records:
        last_update = datetime.fromisoformat(row["last_update"])
        cursor.execute("""
            INSERT INTO car_sales (id, car_model, seller_name, buyer_name, price, sale_date, last_update)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                car_model=VALUES(car_model),
                seller_name=VALUES(seller_name),
                buyer_name=VALUES(buyer_name),
                price=VALUES(price),
                sale_date=VALUES(sale_date),
                last_update=VALUES(last_update)
        """, (
            row["id"],
            row["car_model"],
            row["seller_name"],
            row["buyer_name"],
            row["price"],
            row["sale_date"],
            last_update
        ))
    conn.commit()
    conn.close()
    print(f"Loaded {len(records)} rows into MySQL.")

def update_last_sync(**context):
    """After successful load, update Airflow Variable to current timestamp."""
    now = datetime.utcnow().isoformat()
    Variable.set(LAST_SYNC_VAR, now)
    print(f"Updated last_sync_time to {now}")

with DAG(
    'sync_postgres_to_mysql_incremental',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0/10 * * * *',  
    catchup=False,
    default_args=default_args,
    description='Incremental ETL from Postgres to MySQL using last_update'
) as dag:

    extract = PythonOperator(
        task_id='extract_incremental',
        python_callable=extract_incremental,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load_to_mysql',
        python_callable=load_to_mysql,
        provide_context=True
    )

    update_sync = PythonOperator(
        task_id='update_last_sync',
        python_callable=update_last_sync,
        provide_context=True
    )

    extract >> load >> update_sync
