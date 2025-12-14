from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import vertica_python

# Конфигурация Vertica
VERTICA_CONN = {
    
}

def load_global_metrics(ds=None, **kwargs):
    """
    Загружает данные из стейджа в global_metrics.
    Если ds передана — берём данные за эту дату (для catchup/старых запусков),
    иначе берём за вчерашний день.
    """
    conn = vertica_python.connect(**VERTICA_CONN)
    cur = conn.cursor()
    
    # Определяем дату для вставки
    if ds:
        load_date = ds
    else:
        cur.execute("SELECT CURRENT_DATE - 1;")
        load_date = cur.fetchone()[0]
    
    # Удаляем старые данные за дату
    delete_sql = f"""
        DELETE FROM STV2025061321__DWH.global_metrics
        WHERE date_update = '{load_date}';
    """
    cur.execute(delete_sql)
    
    # Вставка агрегатов
    insert_sql = f"""
        INSERT INTO STV2025061321__DWH.global_metrics (
            date_update,
            currency_from,
            amount_total,
            cnt_transactions,
            avg_transactions_per_account,
            cnt_accounts_make_transactions
        )
        SELECT
            t.transaction_dt::date AS date_update,
            t.currency_code AS currency_from,
            SUM(t.amount * c.currency_with_div) AS amount_total,
            COUNT(*) AS cnt_transactions,
            COUNT(*) / COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
            COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
        FROM STV2025061321__STAGING.transactions t
        JOIN STV2025061321__STAGING.currencies c
            ON t.currency_code = c.currency_code
        WHERE t.transaction_dt::date = '{load_date}'
          AND t.account_number_from >= 0
        GROUP BY t.transaction_dt::date, t.currency_code;
    """
    
    cur.execute(insert_sql)
    conn.close()
    print(f"Inserted global_metrics for {load_date}")

# DAG
with DAG(
    dag_id="staging_to_global_metrics",
    start_date=datetime(2022, 10, 1),   # старт для catchup — 1 октября 2022
    end_date=datetime(2022, 10, 31),    # конец октября 2022
    schedule_interval="@daily",
    catchup=True,                        # заполняем пропущенные дни (октябрь)
    tags=["dwh", "global_metrics"]
) as dag:

    load_metrics = PythonOperator(
        task_id="load_global_metrics",
        python_callable=load_global_metrics,
        provide_context=True
    )

    load_metrics
