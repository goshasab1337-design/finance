import logging
from datetime import datetime

import clickhouse_connect
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# Подставь свои реальные таблицы и колонку времени
PG_TABLE = "stock_prices"
CH_TABLE = "stock_prices"
DATETIME_COLUMN = "price_time"

CH_COLUMNS = [
    "ticker",
    DATETIME_COLUMN,
    "open",
    "high",
    "low",
    "close",
    "volume",
    "interval_value",
]


def get_postgres_engine(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    return create_engine(conn.get_uri())


def get_clickhouse_client(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    return clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port or 8123,
        username=conn.login or "airflow",
        password=conn.password or "airflow",
        database=conn.schema or "default",
    )


def ensure_clickhouse_table(client):
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CH_TABLE} (
            ticker String,
            {DATETIME_COLUMN} DateTime,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            interval_value Int32
        )
        ENGINE = MergeTree()
        ORDER BY (ticker, interval_value, {DATETIME_COLUMN})
        """
    )


def get_last_loaded_dt(client, ticker: str, interval: int):
    ticker_safe = ticker.replace("'", "\\'")
    result = client.query(
        f"""
        SELECT max({DATETIME_COLUMN})
        FROM {CH_TABLE}
        WHERE ticker = '{ticker_safe}'
          AND interval_value = {interval}
        """
    )
    value = result.result_rows[0][0]
    return value or datetime(1970, 1, 1)


def transfer_one_ticker(
    ticker: str,
    interval: int,
    pg_conn_id: str = "pg_finance",
    ch_conn_id: str = "ch_finance",
):
    logger.info("Start transfer: ticker=%s, interval=%s", ticker, interval)

    pg_engine = get_postgres_engine(pg_conn_id)
    ch_client = get_clickhouse_client(ch_conn_id)

    ensure_clickhouse_table(ch_client)
    last_loaded_dt = get_last_loaded_dt(ch_client, ticker, interval)

    sql = text(
        f"""
        SELECT
            ticker,
            {DATETIME_COLUMN},
            open,
            high,
            low,
            close,
            volume,
            "interval" AS interval_value
        FROM {PG_TABLE}
        WHERE ticker = :ticker
          AND "interval" = :interval
          AND {DATETIME_COLUMN} > :last_loaded_dt
        ORDER BY {DATETIME_COLUMN}
        """
    )

    with pg_engine.connect() as conn:
        result = conn.execute(
            sql,
            {
                "ticker": ticker,
                "interval": interval,
                "last_loaded_dt": last_loaded_dt,
            },
        )
        rows = [tuple(row) for row in result.fetchall()]

    if not rows:
        logger.info("No new rows for %s (%s)", ticker, interval)
        return

    ch_client.insert(
        CH_TABLE,
        rows,
        column_names=CH_COLUMNS,
    )

    logger.info(
        "Inserted %s rows into ClickHouse for %s (%s)",
        len(rows),
        ticker,
        interval,
    )