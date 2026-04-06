from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from config import tickers
from stock_transfer import transfer_one_ticker


with DAG(
    dag_id="pg_to_clickhouse_stocks",
    start_date=datetime(2026, 4, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["stocks", "postgres", "clickhouse"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for item in tickers:
        task = PythonOperator(
            task_id=f"load_{item['ticker'].lower()}_{item['interval']}",
            python_callable=transfer_one_ticker,
            op_kwargs={
                "ticker": item["ticker"],
                "interval": item["interval"],
                "pg_conn_id": "pg_finance",
                "ch_conn_id": "ch_finance",
            },
        )

        start >> task >> end