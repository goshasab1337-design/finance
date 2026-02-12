from airflow import DAG  # основной класс DAG
from airflow.operators.python import PythonOperator  # оператор для запуска python-функции
from airflow.providers.postgres.hooks.postgres import PostgresHook  # хук для Postgres через Airflow Connections

import yfinance as yf  # скачивание котировок
import pandas as pd  # удобная работа с NaN и типами
from datetime import datetime, timedelta  # даты и сдвиг на -1 день
from pathlib import Path  # для добавления текущей папки в sys.path
import sys  # чтобы расширить путь импорта
import logging  # логирование в Airflow

sys.path.append(str(Path(__file__).parent))  # добавляем папку dags_finance в импорт питона
from config import TICKERS  # берём тикеры из конфига

logger = logging.getLogger("airflow.task")  # стандартный логгер Airflow

POSTGRES_CONN_ID = "pg_airflow"  # Conn Id, который ты создал в Airflow Admin → Connections
TABLE_NAME = "public.moex_prices"  # таблица, которую мы создали в Postgres

INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    ticker, dt, open, high, low, close, adj_close, volume, interval_start, interval_end
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ticker, dt) DO NOTHING;
"""  # вставка с защитой от дублей по PK (ticker, dt)

def get_data(**kwargs):  # функция, которую вызывает PythonOperator
    interval_start = kwargs["data_interval_start"] - timedelta(days=1)  # начало интервала (-1 день)
    interval_end = kwargs["data_interval_end"]  # конец интервала

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)  # создаём хук
    conn = pg.get_conn()  # открываем соединение
    cur = conn.cursor()  # курсор для SQL

    total_inserted = 0  # счётчик вставок

    for ticker in TICKERS:  # перебор тикеров
        try:
            df = yf.download(ticker, start=interval_start, end=interval_end, progress=False)  # качаем данные
        except Exception as e:
            logger.warning(f"Skip ticker={ticker}: download error: {e}")  # если ошибка — пропускаем тикер
            continue

        if df is None or df.empty:
            logger.warning(f"Skip ticker={ticker}: empty dataframe")  # если нет данных — пропускаем
            continue

        df = df.reset_index()  # делаем дату колонкой (а не индексом)

        # приводим имена колонок к единому виду под нашу таблицу
        df = df.rename(columns={
            "Date": "dt",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        })

        # если вдруг yfinance отдал не "Date", а "Datetime" (редко), подстрахуемся
        if "dt" not in df.columns and "Datetime" in df.columns:
            df = df.rename(columns={"Datetime": "dt"})

        if "dt" not in df.columns:
            logger.warning(f"Skip ticker={ticker}: no dt column after reset_index()")
            continue

        rows = []  # батч строк для вставки

        for r in df.itertuples(index=False):  # быстрый обход строк DataFrame
            dt = getattr(r, "dt")  # дата/время свечи

            open_ = getattr(r, "open", None)
            high_ = getattr(r, "high", None)
            low_ = getattr(r, "low", None)
            close_ = getattr(r, "close", None)
            adj_close_ = getattr(r, "adj_close", None)
            volume_ = getattr(r, "volume", None)

            # заменяем NaN на None (Postgres не понимает NaN как NULL)
            def n2n(x):
                return None if x is None or (isinstance(x, float) and pd.isna(x)) else x

            rows.append((
                ticker,                    # ticker
                dt,                        # dt
                n2n(open_),                # open
                n2n(high_),                # high
                n2n(low_),                 # low
                n2n(close_),               # close
                n2n(adj_close_),           # adj_close
                None if pd.isna(volume_) else int(volume_),  # volume
                interval_start,            # interval_start
                interval_end,              # interval_end
            ))

        cur.executemany(INSERT_SQL, rows)  # вставляем пачкой
        conn.commit()  # фиксируем транзакцию

        # rowcount тут может быть “примерным” из-за ON CONFLICT, но для ориентира ок
        logger.info(f"Loaded ticker={ticker}, rows={len(rows)}")
        total_inserted += len(rows)

    cur.close()  # закрываем курсор
    conn.close()  # закрываем соединение

    logger.info(f"Done. Total processed rows: {total_inserted}")
    return total_inserted  # вернём, сколько строк обработали

with DAG(
    dag_id="my_dag",
    start_date=datetime(2026, 1, 1),
    end_date=None,
    schedule="@daily",
    catchup=True,
) as dag:
    task_1 = PythonOperator(
        task_id="load_moex_prices_to_postgres",
        python_callable=get_data,
    )

    task_1
