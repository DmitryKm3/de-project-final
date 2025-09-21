from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import vertica_python

# Конфиг Vertica
VERTICA_CONN = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'user': 'stv2025061321',
    'password': '5BlrjsKfI9HiZBt',
    'database': 'dwh',
    'autocommit': True
}

# Таблицы в Vertica
STG_TRANSACTIONS = "STV2025061321__STAGING.transactions"
STG_CURRENCIES = "STV2025061321__STAGING.currencies"

# DAG
with DAG(
    dag_id="pg_to_vertica_staging",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "postgres", "vertica"]
) as dag:

    def load_transactions_to_vertica(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres")
        records = hook.get_records("""
            SELECT operation_id,
                   account_number_from,
                   account_number_to,
                   currency_code,
                   country,
                   status,
                   transaction_type,
                   amount,
                   transaction_dt
            FROM public.transactions;
        """)
        if not records:
            print(f"No new data for {STG_TRANSACTIONS}")
            return

        conn = vertica_python.connect(**VERTICA_CONN)
        cur = conn.cursor()
        insert_sql = f"""
            INSERT INTO {STG_TRANSACTIONS} 
            (operation_id, account_number_from, account_number_to, currency_code,
             country, status, transaction_type, amount, transaction_dt)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cur.executemany(insert_sql, records)
        conn.close()
        print(f"Loaded {len(records)} records into {STG_TRANSACTIONS}")

    def load_currencies_to_vertica(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres")
        records = hook.get_records("""
            SELECT date_update,
                   currency_code,
                   currency_code_with,
                   currency_with_div
            FROM public.currencies;
        """)
        if not records:
            print(f"No new data for {STG_CURRENCIES}")
            return

        conn = vertica_python.connect(**VERTICA_CONN)
        cur = conn.cursor()
        insert_sql = f"""
            INSERT INTO {STG_CURRENCIES} 
            (date_update, currency_code, currency_code_with, currency_with_div)
            VALUES (%s,%s,%s,%s)
        """
        cur.executemany(insert_sql, records)
        conn.close()
        print(f"Loaded {len(records)} records into {STG_CURRENCIES}")

    # Операторы
    load_tx = PythonOperator(
        task_id="load_transactions_to_vertica",
        python_callable=load_transactions_to_vertica,
        provide_context=True
    )

    load_cur = PythonOperator(
        task_id="load_currencies_to_vertica",
        python_callable=load_currencies_to_vertica,
        provide_context=True
    )

    # Последовательность
    load_tx
    load_cur
