from airflow import DAG  # основной класс DAG
from airflow.operators.python import PythonOperator  # оператор для запуска python-функции
from airflow.providers.postgres.hooks.postgres import PostgresHook  # хук для Postgres через Airflow Connections

import yfinance as yf  # скачивание котировок
import pandas as pd  # удобная работа с NaN и типами
import requests  # для MOEX ISS fallback
from datetime import datetime, timedelta  # даты и сдвиг на -1 день
from pathlib import Path  # для добавления текущей папки в sys.path
import sys  # чтобы расширить путь импорта
import logging  # логирование в Airflow

sys.path.append(str(Path(__file__).parent))  # добавляем папку с dag.py в импорт питона
from config import TICKERS  # берём тикеры из конфига

logger = logging.getLogger("airflow.task")  # стандартный логгер Airflow

POSTGRES_CONN_ID = "pg_airflow"  # Conn Id в Airflow Admin → Connections
TABLE_NAME = "public.moex_prices"  # таблица в Postgres

INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    ticker, dt, open, high, low, close, adj_close, volume, interval_start, interval_end
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ticker, dt) DO NOTHING;
"""


def _moex_iss_daily_candle(secid: str, day_str: str) -> pd.DataFrame:
    # secid: "SBER", day_str: "2026-02-12"
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{secid}/candles.json"
    r = requests.get(url, params={"from": day_str, "till": day_str, "interval": 24}, timeout=20)
    r.raise_for_status()

    payload = r.json().get("candles", {})
    cols = payload.get("columns", [])
    data = payload.get("data", [])

    if not data:
        return pd.DataFrame()

    tmp = pd.DataFrame(data, columns=cols)

    # приводим к единому формату под вставку
    df = pd.DataFrame(
        {
            "dt": pd.to_datetime(tmp["begin"], utc=True),
            "open": tmp.get("open"),
            "high": tmp.get("high"),
            "low": tmp.get("low"),
            "close": tmp.get("close"),
            "adj_close": None,  # у ISS нет Adj Close
            "volume": tmp.get("volume"),
        }
    )
    return df


def _to_scalar(x):
    # Series -> первый элемент (если колонки продублировались)
    if isinstance(x, pd.Series):
        x = x.iloc[0] if len(x) else None

    # NaN/NaT -> None
    try:
        if x is None or pd.isna(x):
            return None
    except Exception:
        pass

    # Timestamp -> python datetime
    if isinstance(x, pd.Timestamp):
        x = x.to_pydatetime()

    return x


def _download_yfinance_any(ticker: str, start_date, end_date) -> tuple[pd.DataFrame, str | None]:
    """
    Пробуем скачать котировки через yfinance:
    - если тикер уже с суффиксом (есть ".") -> пробуем только его
    - иначе пробуем сначала как есть, затем с ".ME"
    Возвращаем (df, used_ticker)
    """
    candidates = [ticker] if "." in ticker else [ticker, f"{ticker}.ME"]

    for t in candidates:
        try:
            df = yf.download(
                t,
                start=start_date,
                end=end_date,
                interval="1d",
                progress=False,
                auto_adjust=False,
            )
        except Exception as e:
            logger.warning(f"yfinance failed ticker={t}: {e}")
            df = pd.DataFrame()

        if df is not None and not df.empty:
            return df, t

    return pd.DataFrame(), None


def get_data(**kwargs):
    # берём интервал Airflow и делаем "вчера"
    interval_start = kwargs["data_interval_start"] - timedelta(days=1)
    interval_end = kwargs["data_interval_end"]

    # для MOEX ISS нужен день строкой
    day_str = interval_start.date().strftime("%Y-%m-%d")

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    total_inserted = 0

    for ticker in TICKERS:
        # secid для MOEX ISS: если тикер вида SBER.ME -> берём SBER, иначе как есть
        secid = ticker.split(".")[0] if "." in ticker else ticker

        # 1) пробуем yfinance (ВАЖНО: сначала как есть, потом .ME)
        df, used_yf = _download_yfinance_any(ticker, interval_start.date(), interval_end.date())

        # 2) если пусто — fallback на MOEX ISS (только для MOEX-тикеров)
        if df is None or df.empty:
            logger.warning(
                f"yfinance empty tickers={[ticker] if '.' in ticker else [ticker, ticker + '.ME']} "
                f"-> fallback to MOEX ISS secid={secid}"
            )
            try:
                df = _moex_iss_daily_candle(secid, day_str)
                used_yf = None
            except Exception as e:
                logger.warning(f"MOEX ISS failed secid={secid} day={day_str}: {e}")
                continue

        if df is None or df.empty:
            logger.warning(f"Skip ticker={ticker}: empty dataframe from all sources")
            continue

        if used_yf:
            logger.info(f"yfinance ok ticker={used_yf}, rows={len(df)}")
        else:
            logger.info(f"MOEX ISS ok secid={secid}, rows={len(df)}")

        # если вдруг MultiIndex колонки — уплощаем
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]

        df = df.reset_index()

        # приводим имена колонок к единому виду
        df = df.rename(
            columns={
                "Date": "dt",
                "Datetime": "dt",
                "index": "dt",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
            }
        )

        # убираем дубли колонок (ключевой фикс от Series внутри строки)
        df = df.loc[:, ~df.columns.duplicated()]

        if "dt" not in df.columns:
            logger.warning(f"Skip ticker={ticker}: no dt column after reset_index(); columns={list(df.columns)}")
            continue

        rows = []

        for _, r in df.iterrows():
            dt = _to_scalar(r.get("dt"))

            # так как у нас дневные свечи — безопаснее хранить dt как DATE (убираем tz/time)
            if isinstance(dt, datetime):
                dt = dt.date()

            open_ = _to_scalar(r.get("open"))
            high_ = _to_scalar(r.get("high"))
            low_ = _to_scalar(r.get("low"))
            close_ = _to_scalar(r.get("close"))
            adj_close_ = _to_scalar(r.get("adj_close"))

            volume_ = _to_scalar(r.get("volume"))
            volume_val = None if volume_ is None else int(volume_)

            rows.append(
                (
                    secid,  # ticker (без .ME)
                    dt,  # dt (DATE)
                    open_,  # open
                    high_,  # high
                    low_,  # low
                    close_,  # close
                    adj_close_,  # adj_close
                    volume_val,  # volume
                    interval_start,  # interval_start
                    interval_end,  # interval_end
                )
            )

        try:
            cur.executemany(INSERT_SQL, rows)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"DB insert failed for ticker={ticker}: {e}")
            raise

        logger.info(f"Loaded ticker={secid}, rows={len(rows)}")
        total_inserted += len(rows)

    cur.close()
    conn.close()

    logger.info(f"Done. Total processed rows: {total_inserted}")
    return total_inserted


with DAG(
    dag_id="my_dag1",
    start_date=datetime(2024, 1, 1),
    end_date=None,
    schedule="@daily",
    catchup=True,
) as dag:
    task_1 = PythonOperator(
        task_id="load_moex_prices_to_postgres",
        python_callable=get_data,
    )

    task_1
