import json
import logging
import time

import websocket
from kafka import KafkaProducer

from config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("binance_producer")

LOG_EVERY_N_TRADES = 100
MAX_RECONNECT_DELAY_SECONDS = 60


def parse_trade(message):
    """Turn a Binance trade message into our event, or None if it isn't a trade."""
    data = json.loads(message)
    if data.get("e") != "trade":
        return None
    return {
        "symbol": data["s"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "timestamp": data["T"],
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    sent = 0

    def on_message(ws, message):
        nonlocal sent
        try:
            event = parse_trade(message)
        except (ValueError, KeyError) as e:
            log.warning("Skipping malformed message (%s): %s", e, message[:200])
            return
        if event is None:
            return
        producer.send(settings.CRYPTO_TOPIC, key=event["symbol"], value=event)
        sent += 1
        if sent % LOG_EVERY_N_TRADES == 0:
            log.info("Sent %d trades — latest %s @ $%.2f", sent, event["symbol"], event["price"])

    def on_error(ws, error):
        log.error("WebSocket error: %s", error)

    def on_close(ws, close_status_code, close_msg):
        log.warning("Connection closed: %s %s", close_status_code, close_msg)

    def on_open(ws):
        log.info("Connected to Binance! Streaming BTC prices...")

    # Binance drops every connection after 24h (and networks fail), so always reconnect.
    delay = 1
    try:
        while True:
            started = time.monotonic()
            ws = websocket.WebSocketApp(
                settings.BINANCE_WS_URL,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open,
            )
            ws.run_forever(ping_interval=60, ping_timeout=10)
            # Reset the backoff after a connection that stayed up for a while.
            if time.monotonic() - started > MAX_RECONNECT_DELAY_SECONDS:
                delay = 1
            log.info("Reconnecting in %ds...", delay)
            time.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY_SECONDS)
    except KeyboardInterrupt:
        log.info("Stopping...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
