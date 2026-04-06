from datetime import datetime

import clickhouse_connect
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from config.stocks_config import (
    ticker,
    POSTGRES_CONN_ID,
    CLICKHOUSE_CONN_ID,
    SOURCE_TABLE,
    TARGET_TABLE,
    TICKER_COLUMN,
    DATE_COLUMN,
)


def transfer_data():
    # колонки, которые читаем из Postgres и пишем в ClickHouse
    columns = [
        "ticker",
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
    ]

    # хук Postgres
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # подключение к ClickHouse из Airflow Connection
    ch_conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    ch_client = clickhouse_connect.get_client(
        host=ch_conn.host,
        port=ch_conn.port or 8123,
        username=ch_conn.login or "default",
        password=ch_conn.password or "",
        database=ch_conn.schema or "default",
    )

    try:
        with pg_hook.get_conn() as pg_conn:
            with pg_conn.cursor() as cursor:
                for stock in ticker:
                    # берем последнюю дату по тикеру из ClickHouse
                    stock_safe = stock.replace("'", "''")
                    last_dt_sql = (
                        f"SELECT max({DATE_COLUMN}) "
                        f"FROM {TARGET_TABLE} "
                        f"WHERE {TICKER_COLUMN} = '{stock_safe}'"
                    )
                    last_dt = ch_client.query(last_dt_sql).result_rows[0][0]

                    # тянем только новые строки
                    if last_dt:
                        cursor.execute(
                            f"""
                            SELECT {", ".join(columns)}
                            FROM {SOURCE_TABLE}
                            WHERE {TICKER_COLUMN} = %s
                              AND {DATE_COLUMN} > %s
                            ORDER BY {DATE_COLUMN}
                            """,
                            (stock, last_dt),
                        )
                    else:
                        cursor.execute(
                            f"""
                            SELECT {", ".join(columns)}
                            FROM {SOURCE_TABLE}
                            WHERE {TICKER_COLUMN} = %s
                            ORDER BY {DATE_COLUMN}
                            """,
                            (stock,),
                        )

                    rows = cursor.fetchall()

                    # если новых данных нет — пропускаем
                    if not rows:
                        continue

                    # вставка в ClickHouse
                    ch_client.insert(TARGET_TABLE, rows, column_names=columns)
    finally:
        ch_client.close()


with DAG(
    dag_id="postgres_to_clickhouse_stocks",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "airflow", "depends_on_past": False},
    tags=["postgres", "clickhouse", "stocks"],
) as dag:
    transfer_task = PythonOperator(
        task_id="transfer_data_from_postgres_to_clickhouse",
        python_callable=transfer_data,
    )