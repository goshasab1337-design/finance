# импортируем logging, чтобы писать логи в Airflow
import logging

# импортируем clickhouse_connect для подключения к ClickHouse
import clickhouse_connect

# импортируем BaseHook, чтобы читать connection ClickHouse из Airflow
from airflow.hooks.base import BaseHook

# импортируем PostgresHook, чтобы читать данные из Postgres
from airflow.providers.postgres.hooks.postgres import PostgresHook

# импортируем настройки из конфига
from config.stocks_config import (
    ticker,               # список тикеров
    POSTGRES_CONN_ID,     # id подключения к Postgres
    CLICKHOUSE_CONN_ID,   # id подключения к ClickHouse
    SOURCE_TABLE,         # таблица-источник в Postgres
    TARGET_TABLE,         # таблица-приёмник в ClickHouse
    TICKER_COLUMN,        # колонка с тикером
    DATE_COLUMN,          # колонка с датой
)

# создаём логгер для этого модуля
logger = logging.getLogger(__name__)


def transfer_postgres_to_clickhouse():
    """
    Главная функция второй таски DAG.

    Что делает:
    1. подключается к Postgres
    2. подключается к ClickHouse
    3. по каждому тикеру смотрит max дату уже в ClickHouse
    4. тянет из Postgres только новые строки
    5. вставляет их в ClickHouse

    Зачем нужна:
    - это второй этап пайплайна
    - она отделена от DAG-файла, чтобы DAG был коротким и читаемым
    """
    # пишем в лог, что начинаем вторую таску
    logger.info("Start transferring data from Postgres to ClickHouse")

    # список колонок, которые читаем из Postgres и пишем в ClickHouse
    columns = [
        "ticker",
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
    ]

    # создаём хук для подключения к Postgres
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # читаем connection ClickHouse из Airflow
    ch_conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)

    # создаём клиента ClickHouse
    ch_client = clickhouse_connect.get_client(
        # хост ClickHouse
        host=ch_conn.host,

        # порт ClickHouse, если он не задан, используем 8123
        port=ch_conn.port or 8123,

        # логин ClickHouse
        username=ch_conn.login or "default",

        # пароль ClickHouse
        password=ch_conn.password or "",

        # база ClickHouse
        database=ch_conn.schema or "default",
    )

    try:
        # открываем подключение к Postgres
        with pg_hook.get_conn() as pg_conn:
            # открываем курсор Postgres
            with pg_conn.cursor() as cursor:
                # идём по каждому тикеру из конфига
                for stock in ticker:
                    # экранируем тикер для SQL-запроса в ClickHouse
                    stock_safe = stock.replace("'", "''")

                    # собираем SQL для поиска max даты по тикеру в ClickHouse
                    last_dt_sql = (
                        f"SELECT max({DATE_COLUMN}) "
                        f"FROM {TARGET_TABLE} "
                        f"WHERE {TICKER_COLUMN} = '{stock_safe}'"
                    )

                    # выполняем запрос в ClickHouse
                    last_dt = ch_client.query(last_dt_sql).result_rows[0][0]

                    # если в ClickHouse уже есть строки по тикеру,
                    # тянем из Postgres только даты новее max даты
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
                        # если в ClickHouse по тикеру данных ещё нет,
                        # тянем все строки из Postgres
                        cursor.execute(
                            f"""
                            SELECT {", ".join(columns)}
                            FROM {SOURCE_TABLE}
                            WHERE {TICKER_COLUMN} = %s
                            ORDER BY {DATE_COLUMN}
                            """,
                            (stock,),
                        )

                    # получаем строки из Postgres
                    rows = cursor.fetchall()

                    # если новых строк нет, пропускаем тикер
                    if not rows:
                        logger.info("Skip ticker %s: no new rows for ClickHouse", stock)
                        continue

                    # вставляем строки в ClickHouse
                    ch_client.insert(
                        TARGET_TABLE,
                        rows,
                        column_names=columns,
                    )

                    # пишем в лог, сколько строк перелили
                    logger.info(
                        "Transferred %s rows from Postgres to ClickHouse for ticker %s",
                        len(rows),
                        stock,
                    )

        # пишем в лог об успешном окончании таски
        logger.info("Finish transferring data from Postgres to ClickHouse")

    finally:
        # обязательно закрываем клиент ClickHouse
        ch_client.close()