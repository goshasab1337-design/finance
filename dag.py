from airflow import DAG
from airflow.operators.python import PythonOperator
import os  # для работы с путями и папками
import yfinance as yf 
from datetime import datetime, timedelta
from pathlib import Path  # чтобы добавить текущую папку в sys.path
import sys  # чтобы расширить путь импорта

sys.path.append(str(Path(__file__).parent))  # добавляем папку dags_finance в импорты Python
from config import TICKERS, OUT_DIR  # импортируем тикеры и папку вывода из конфига

def get_data(**kwargs):  # функция таска
    start = kwargs["data_interval_start"] - timedelta(days=1)  # <-- начало интервала -1 день
    end = kwargs["data_interval_end"]  # конец интервала

    os.makedirs(OUT_DIR, exist_ok=True)  # создаём папку для выгрузки
    day = start.strftime("%Y-%m-%d")  # дата для имени файла

    for ticker in TICKERS:  # перебираем тикеры
        df = yf.download(ticker, start=start, end=end, progress=False)  # скачиваем котировки
        df = df.reset_index()  # переносим дату из индекса в колонку
        path = os.path.join(OUT_DIR, f"{day}_{ticker}.json")  # путь до json
        df.to_json(path, orient="records", date_format="iso", force_ascii=False)  # сохраняем в json

with DAG(
    dag_id="my_dag",
    start_date=datetime(2026, 1, 1),
    end_date=None,
    schedule="@daily",
    catchup=True,
) as dag:
    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=get_data,
    )

    task_1
