# импортируем logging, чтобы писать логи в Airflow
import logging

# импортируем date, чтобы брать сегодняшнюю дату
from datetime import date

# импортируем timedelta, чтобы считать следующий день после max даты
from datetime import timedelta

# импортируем requests для HTTP-запросов в MOEX API
import requests

# импортируем HTTPAdapter для настройки retry-политики у requests
from requests.adapters import HTTPAdapter

# импортируем Retry для автоматических повторов HTTP-запросов
from urllib3.util.retry import Retry

# импортируем PostgresHook для подключения к Postgres через Airflow Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook

# импортируем настройки из конфига
from config.stocks_config import (
    ticker,              # список тикеров
    POSTGRES_CONN_ID,    # id подключения к Postgres
    SOURCE_TABLE,        # таблица в Postgres, куда пишем данные
    TICKER_COLUMN,       # колонка с тикером
    DATE_COLUMN,         # колонка с датой
    MOEX_BASE_URL,       # базовый URL MOEX API
    ENGINE,              # engine в MOEX API
    MARKET,              # market в MOEX API
    BOARD,               # board в MOEX API
    START_DATE,          # дата, с которой начинаем историю, если данных еще нет
    REQUEST_TIMEOUT,     # timeout для HTTP-запросов
)

# создаём логгер для этого модуля
logger = logging.getLogger(__name__)


def build_moex_session():
    """
    Эта функция создаёт requests.Session с retry-политикой.

    Зачем нужна:
    - чтобы не делать "голый" requests.get без повторов
    - чтобы при временных сетевых ошибках запрос повторялся автоматически
    - чтобы использовать один session-объект для нескольких запросов
    """
    # создаём session
    session = requests.Session()

    # настраиваем retries для HTTP-запросов
    retries = Retry(
        # общее число повторных попыток
        total=5,

        # количество повторов при проблемах соединения
        connect=5,

        # количество повторов при ошибках чтения ответа
        read=5,

        # коэффициент увеличения паузы между повторами
        backoff_factor=2,

        # список HTTP-статусов, при которых запрос надо повторять
        status_forcelist=[429, 500, 502, 503, 504],

        # разрешаем ретраи только для GET-запросов
        allowed_methods=["GET"],
    )

    # создаём адаптер с заданными ретраями
    adapter = HTTPAdapter(max_retries=retries)

    # вешаем адаптер на https-запросы
    session.mount("https://", adapter)

    # вешаем адаптер на http-запросы
    session.mount("http://", adapter)

    # добавляем заголовки к запросам
    session.headers.update(
        {
            # user-agent, чтобы запрос выглядел как нормальный клиент
            "User-Agent": "airflow-moex-loader/1.0",

            # просим JSON
            "Accept": "application/json",
        }
    )

    # возвращаем готовый session
    return session


def get_start_date(cursor, stock: str) -> str:
    """
    Эта функция определяет, с какой даты нужно догружать данные по тикеру.

    Логика:
    - если по тикеру в Postgres данных нет, берём START_DATE из конфига
    - если данные есть, берём следующий день после max(trade_date)

    Зачем нужна:
    - чтобы не тянуть всю историю заново каждый день
    - чтобы делать инкрементальную загрузку
    """
    # выполняем SQL:
    # ищем максимальную дату по конкретному тикеру в Postgres
    cursor.execute(
        f"""
        SELECT max({DATE_COLUMN})
        FROM {SOURCE_TABLE}
        WHERE {TICKER_COLUMN} = %s
        """,
        (stock,),
    )

    # забираем одно значение из результата запроса
    max_dt = cursor.fetchone()[0]

    # если по тикеру данных ещё нет, возвращаем дату из конфига
    if not max_dt:
        return START_DATE

    # считаем следующий день после уже загруженной максимальной даты
    next_dt = max_dt + timedelta(days=1)

    # возвращаем дату в формате YYYY-MM-DD
    return next_dt.strftime("%Y-%m-%d")


def fetch_moex_history(session, stock: str, from_date: str, till_date: str):
    """
    Эта функция ходит в MOEX API и забирает историю по одному тикеру.

    Параметры:
    - session: requests.Session с ретраями
    - stock: тикер, например SBER
    - from_date: дата начала периода
    - till_date: дата конца периода

    Зачем нужна:
    - чтобы изолировать всю работу с внешним API в одной функции
    - чтобы load_moex_to_postgres была более читаемой
    """
    # собираем endpoint MOEX API для конкретного тикера
    url = (
        f"{MOEX_BASE_URL}/history/engines/{ENGINE}/markets/{MARKET}"
        f"/boards/{BOARD}/securities/{stock}.json"
    )

    # создаём пустой список строк,
    # который потом вернём для вставки в Postgres
    rows = []

    # start нужен для пагинации в MOEX API
    start = 0

    # крутим цикл, пока API возвращает данные
    while True:
        # собираем query-параметры запроса
        params = {
            # дата начала периода
            "from": from_date,

            # дата конца периода
            "till": till_date,

            # смещение для пагинации
            "start": start,

            # отключаем лишние метаданные
            "iss.meta": "off",

            # просим только нужные поля
            "history.columns": "TRADEDATE,OPEN,LOW,HIGH,CLOSE,VOLUME",
        }

        # отправляем GET-запрос в MOEX API
        response = session.get(
            url,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        # если пришёл неуспешный HTTP-статус, выбрасываем исключение
        response.raise_for_status()

        # преобразуем JSON-ответ в Python-словарь
        payload = response.json()

        # достаём массив данных history.data
        data = payload.get("history", {}).get("data", [])

        # если данных больше нет, выходим из цикла
        if not data:
            break

        # идём по каждой строке из ответа MOEX
        for item in data:
            # распаковываем значения в переменные
            trade_date, open_price, low_price, high_price, close_price, volume = item

            # формируем кортеж в том порядке,
            # в котором потом будем вставлять в Postgres
            rows.append(
                (
                    stock,         # тикер
                    trade_date,    # дата торгов
                    open_price,    # цена открытия
                    high_price,    # максимальная цена
                    low_price,     # минимальная цена
                    close_price,   # цена закрытия
                    volume,        # объём
                )
            )

        # увеличиваем offset для получения следующей страницы
        start += len(data)

    # возвращаем все собранные строки
    return rows


def load_moex_to_postgres():
    """
    Главная функция первой таски DAG.

    Что делает:
    1. подключается к Postgres
    2. берёт список тикеров из конфига
    3. для каждого тикера определяет дату начала загрузки
    4. ходит в MOEX API
    5. сохраняет данные в Postgres через upsert

    Зачем нужна:
    - это первый этап пайплайна MOEX -> Postgres -> ClickHouse
    """
    # пишем в лог, что начинаем первую таску
    logger.info("Start loading data from MOEX to Postgres")

    # создаём PostgresHook по connection id из Airflow
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # создаём session для MOEX API с ретраями
    session = build_moex_session()

    # берём сегодняшнюю дату как верхнюю границу загрузки
    till_date = date.today().strftime("%Y-%m-%d")

    try:
        # открываем подключение к Postgres
        with pg_hook.get_conn() as conn:
            # открываем курсор для выполнения SQL
            with conn.cursor() as cursor:
                # идём по каждому тикеру из конфига
                for stock in ticker:
                    # определяем, с какой даты нужно догружать данные
                    from_date = get_start_date(cursor, stock)

                    # если from_date уже больше till_date,
                    # значит догружать нечего
                    if from_date > till_date:
                        logger.info("Skip ticker %s: no new dates to load", stock)
                        continue

                    # пишем в лог, что начинаем загрузку по тикеру
                    logger.info(
                        "Load ticker %s from %s to %s",
                        stock,
                        from_date,
                        till_date,
                    )

                    # забираем историю из MOEX API
                    rows = fetch_moex_history(
                        session=session,
                        stock=stock,
                        from_date=from_date,
                        till_date=till_date,
                    )

                    # если MOEX ничего не вернул, идём к следующему тикеру
                    if not rows:
                        logger.info("No rows returned from MOEX for ticker %s", stock)
                        continue

                    # делаем массовую вставку/обновление в Postgres
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

                    # пишем в лог, сколько строк обработали
                    logger.info("Loaded %s rows to Postgres for ticker %s", len(rows), stock)

            # фиксируем транзакцию в Postgres
            conn.commit()

        # пишем в лог об успешном окончании таски
        logger.info("Finish loading data from MOEX to Postgres")

    finally:
        # закрываем session в любом случае
        session.close()