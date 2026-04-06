from datetime import datetime, date

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from config.stocks_config import (
    ticker,
    POSTGRES_CONN_ID,
    SOURCE_TABLE,
    TICKER_COLUMN,
    DATE_COLUMN,
    MOEX_BASE_URL,
    ENGINE,
    MARKET,
    BOARD,
    START_DATE,
    REQUEST_TIMEOUT,
)


def get_start_date(cursor, stock: str) -> str:
    # берем последнюю дату по тикеру из Postgres
    cursor.execute(
        f"""
        SELECT max({DATE_COLUMN})
        FROM {SOURCE_TABLE}
        WHERE {TICKER_COLUMN} = %s
        """,
        (stock,),
    )
    max_dt = cursor.fetchone()[0]

    # если данных еще нет — стартуем с даты из конфига
    if not max_dt:
        return START_DATE

    # берем последнюю дату, чтобы при повторном запуске обновить ее через upsert
    return max_dt.strftime("%Y-%m-%d")


def fetch_moex_history(stock: str, from_date: str, till_date: str):
    # endpoint истории по бумаге
    url = (
        f"{MOEX_BASE_URL}/history/engines/{ENGINE}/markets/{MARKET}"
        f"/boards/{BOARD}/securities/{stock}.json"
    )

    rows = []
    start = 0

    while True:
        params = {
            "from": from_date,
            "till": till_date,
            "start": start,
            "iss.meta": "off",
            "history.columns": "TRADEDATE,OPEN,LOW,HIGH,CLOSE,VOLUME",
        }

        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        payload = response.json()
        data = payload.get("history", {}).get("data", [])

        # если данных больше нет — выходим
        if not data:
            break

        for item in data:
            trade_date, open_price, low_price, high_price, close_price, volume = item

            rows.append(
                (
                    stock,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                )
            )

        # пагинация по start
        start += len(data)

    return rows


def load_moex_to_postgres():
    # подключение к Postgres
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            till_date = date.today().strftime("%Y-%m-%d")

            for stock in ticker:
                # определяем дату начала загрузки
                from_date = get_start_date(cursor, stock)

                # тянем историю с MOEX
                rows = fetch_moex_history(
                    stock=stock,
                    from_date=from_date,
                    till_date=till_date,
                )

                # если данных нет — идем дальше
                if not rows:
                    continue

                # upsert в Postgres
                cursor.executemany(
                    f"""
                    INSERT INTO {SOURCE_TABLE} (
                        ticker,
                        trade_date,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, trade_date)
                    DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume
                    """,
                    rows,
                )

        conn.commit()


with DAG(
    dag_id="moex_to_postgres_stocks",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "airflow", "depends_on_past": False},
    tags=["moex", "postgres", "stocks"],
) as dag:
    load_task = PythonOperator(
        task_id="load_moex_to_postgres",
        python_callable=load_moex_to_postgres,
    )