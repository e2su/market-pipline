import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Read the API key from the .env file
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

# List of stock symbols we want to track
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN"]

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_stock_price(symbol):
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()
    print(f"Raw response for {symbol}: {data}")  # temporary debug line

    quote = data.get("Global Quote", {})
    if not quote:
        print(f"No data for {symbol}")
        return None

    return {
        "symbol": quote["01. symbol"],
        "price": float(quote["05. price"]),
        "volume": int(quote["06. volume"]),
        "timestamp": quote["07. latest trading day"]
    }

if __name__ == "__main__":
    print("Starting stock producer...")
    while True:
        for symbol in SYMBOLS:
            event = fetch_stock_price(symbol)
            if event:
                producer.send('stock-prices', value=event)
                print(f"Sent: {event['symbol']} @ ${event['price']}")
            time.sleep(12)