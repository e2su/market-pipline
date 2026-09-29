import json
import logging
import time

import requests
from kafka import KafkaProducer

from config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stock_producer")

ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"
SECONDS_BETWEEN_SYMBOLS = 2
REQUEST_TIMEOUT_SECONDS = 15


class RateLimitError(Exception):
    """Alpha Vantage answered with a rate-limit message instead of data."""


def parse_quote(data):
    """Turn an Alpha Vantage GLOBAL_QUOTE response into our event.

    Returns None when there is no quote (e.g. unknown symbol) and raises
    RateLimitError when the API refuses because the quota is used up.
    """
    # When the quota is exhausted, Alpha Vantage returns HTTP 200 with a "Note"
    # or "Information" message instead of a quote.
    for key in ("Note", "Information"):
        if key in data:
            raise RateLimitError(data[key])
    if "Error Message" in data:
        log.warning("Alpha Vantage error: %s", data["Error Message"])
        return None

    quote = data.get("Global Quote") or {}
    if not quote:
        return None
    return {
        "symbol": quote["01. symbol"],
        "price": float(quote["05. price"]),
        "volume": int(quote["06. volume"]),
        "timestamp": quote["07. latest trading day"],
    }


def fetch_stock_price(symbol):
    response = requests.get(
        ALPHA_VANTAGE_URL,
        params={"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": settings.ALPHA_VANTAGE_API_KEY},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return parse_quote(response.json())


def main():
    if not settings.ALPHA_VANTAGE_API_KEY:
        raise SystemExit("ALPHA_VANTAGE_API_KEY is not set — add it to your .env file.")

    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    last_sent = {}  # symbol -> last event, so unchanged quotes aren't re-sent

    log.info(
        "Starting stock producer for %s (every %ds)...",
        ", ".join(settings.STOCK_SYMBOLS), settings.STOCK_POLL_INTERVAL_SECONDS,
    )
    try:
        while True:
            for symbol in settings.STOCK_SYMBOLS:
                try:
                    event = fetch_stock_price(symbol)
                except RateLimitError as e:
                    log.warning("Rate limited by Alpha Vantage, skipping this round: %s", e)
                    break
                except (requests.RequestException, ValueError, KeyError) as e:
                    log.error("Failed to fetch %s: %s", symbol, e)
                    continue

                if event is None:
                    log.warning("No data for %s", symbol)
                elif event == last_sent.get(symbol):
                    log.info("%s unchanged since last poll, not re-sending", symbol)
                else:
                    producer.send(settings.STOCK_TOPIC, key=symbol, value=event)
                    last_sent[symbol] = event
                    log.info("Sent: %s @ $%.2f", event["symbol"], event["price"])
                time.sleep(SECONDS_BETWEEN_SYMBOLS)

            producer.flush()
            time.sleep(settings.STOCK_POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        log.info("Stopping...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
