"""Shared configuration, read once from the environment / .env file."""
import os

from dotenv import load_dotenv

load_dotenv()


def _get_list(name, default):
    return [item.strip() for item in os.getenv(name, default).split(",") if item.strip()]


# ── Kafka ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CRYPTO_TOPIC = "crypto-prices"
STOCK_TOPIC = "stock-prices"

# ── Producers ────────────────────────────────────────────────────────────────
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
STOCK_SYMBOLS = _get_list("STOCK_SYMBOLS", "AAPL,MSFT,GOOGL,AMZN")
# Free tier = 25 requests/day. 4 symbols every 4 hours = 24 requests/day.
STOCK_POLL_INTERVAL_SECONDS = int(os.getenv("STOCK_POLL_INTERVAL_SECONDS", "14400"))

# ── AWS ──────────────────────────────────────────────────────────────────────
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
S3_CRYPTO_PREFIX = "crypto/"
S3_STOCK_PREFIX = "stocks/"

# ── Processing ───────────────────────────────────────────────────────────────
ANOMALY_THRESHOLD_PCT = float(os.getenv("ANOMALY_THRESHOLD_PCT", "0.5"))

# ── Redshift ─────────────────────────────────────────────────────────────────
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_DB = os.getenv("REDSHIFT_DB", "dev")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_IAM_ROLE = os.getenv("REDSHIFT_IAM_ROLE")
LOADER_INTERVAL_SECONDS = int(os.getenv("LOADER_INTERVAL_SECONDS", "60"))

# ── Dashboard ────────────────────────────────────────────────────────────────
DASHBOARD_REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "30"))


def redshift_connection_args():
    return dict(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        ssl=True,
        sslmode="require",
    )
