print("Step 1: starting")

from kafka import KafkaProducer
import json
print("Step 2: imports done")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("Step 3: Kafka connected!")
print("Step 1: starting")

from kafka import KafkaProducer
import json
import websocket
print("Step 2: imports done")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("Step 3: Kafka connected!")

def on_message(ws, message):
    data = json.loads(message)
    print(f"Received: {data}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"Closed: {close_status_code}")

def on_open(ws):
    print("Step 4: Connected to Binance!")

print("Step 3.5: starting websocket...")
ws = websocket.WebSocketApp(
    "wss://stream.binance.com:9443/ws/btcusdt@trade",
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_open=on_open
)
ws.run_forever()