import requests
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey={API_KEY}"
response = requests.get(url)
print(response.json())