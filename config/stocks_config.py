# тикеры MOEX
ticker = [
    "SBER",
    "GAZP",
    "LKOH",
]

# MOEX ISS
SOURCE_NAME = "moex"
MOEX_BASE_URL = "https://iss.moex.com/iss"
ENGINE = "stock"
MARKET = "shares"
BOARD = "TQBR"
START_DATE = "2024-01-01"
REQUEST_TIMEOUT = 30

# Airflow connections
POSTGRES_CONN_ID = "postgres_default"
CLICKHOUSE_CONN_ID = "clickhouse_default"

# таблицы
SOURCE_TABLE = "public.stock_prices"
TARGET_TABLE = "default.stock_prices"

# поля
TICKER_COLUMN = "ticker"
DATE_COLUMN = "trade_date"