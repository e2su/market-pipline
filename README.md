# ⬡ MARKET_OS — Real-Time Market Data Pipeline

> **Live crypto and stock market data flowing through Kafka → PySpark → AWS S3 → Redshift → Dashboard**

![Stack](https://img.shields.io/badge/Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat&logo=amazonaws&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

---

## 🗺️ What Does This Project Do?

This pipeline does 5 things automatically:

1. **Grabs** live Bitcoin prices every second from Binance
2. **Grabs** stock prices (AAPL, MSFT, GOOGL, AMZN) every minute from Alpha Vantage
3. **Processes** the data using Apache Kafka and PySpark
4. **Saves** it to AWS (S3 data lake + Redshift warehouse)
5. **Shows** everything on a live cyberpunk-style dashboard in your browser

---

## 🏗️ Architecture

```
Binance WebSocket ──┐
                    ├──▶ Apache Kafka ──▶ PySpark ──▶ AWS S3 (Parquet)
Alpha Vantage API ──┘                                      │
                                                           ▼
                                                   Amazon Redshift
                                                           │
                                                           ▼
                                                  Streamlit Dashboard
```

---

## 🧰 What You Need Before Starting

Make sure you have all of these installed on your computer:

| Tool | Why | Download Link |
|---|---|---|
| Python 3.11 | Runs the code | https://www.python.org/downloads/ |
| Docker Desktop | Runs Kafka locally | https://www.docker.com/products/docker-desktop/ |
| VS Code | Code editor | https://code.visualstudio.com/ |
| Java 11+ | Required by PySpark | Already installed if you followed setup |

You also need accounts on:
- **Alpha Vantage** (free) → https://www.alphavantage.co/support/#api-key
- **AWS** (free tier) → https://aws.amazon.com/free/

---

## ⚙️ First Time Setup (Do This Once)

### Step 1 — Download the project
```bash
git clone https://github.com/e2su/market-pipeline.git
cd market-pipeline
```

### Step 2 — Create a virtual environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux
```

### Step 3 — Install all dependencies
```bash
pip install kafka-python websocket-client requests python-dotenv pyspark==3.5.0 boto3 redshift-connector streamlit plotly pandas
```

### Step 4 — Set up your secret keys

Create a file called `.env` in the project root and fill it in:

```
ALPHA_VANTAGE_API_KEY=your_key_here
AWS_ACCESS_KEY_ID=your_key_here
AWS_SECRET_ACCESS_KEY=your_key_here
AWS_REGION=eu-north-1
S3_BUCKET_NAME=your_bucket_name_here
REDSHIFT_HOST=your_redshift_endpoint_here
REDSHIFT_PORT=5439
REDSHIFT_DB=dev
REDSHIFT_USER=admin
REDSHIFT_PASSWORD=your_password_here
```

> 💡 **Where to get these keys:**
> - Alpha Vantage key → sign up at https://www.alphavantage.co/support/#api-key
> - AWS keys → AWS Console → IAM → Users → Security Credentials
> - S3 bucket name → AWS Console → S3
> - Redshift endpoint → AWS Console → Redshift Serverless → your workgroup

### Step 5 — Windows only: Set up Hadoop
```bash
# Download winutils.exe and hadoop.dll from:
# https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.2/bin
# Place both files in C:\hadoop\bin\

setx HADOOP_HOME "C:\hadoop" /M    # Run as Administrator
```

---

## 🚀 How to Run the Project

You need **4 terminals** open at the same time. Think of each terminal as a worker doing one job.

> 💡 **Tip:** In VS Code, press **Ctrl + `** to open a terminal, and **Ctrl + Shift + `** to open another one.

---

### 🟢 Terminal 1 — Start Kafka (the message broker)

```bash
docker-compose up -d
```

✅ Done when you see: `Container kafka Started`

Check it's running:
```bash
docker ps
```
You should see both `kafka` and `zookeeper` listed.

---

### 🟢 Terminal 2 — Start the Binance Producer (live BTC prices)

```bash
venv\Scripts\activate
set HADOOP_HOME=C:\hadoop
python producers/binance_producer.py
```

✅ Done when you see:
```
Connected to Binance! Streaming BTC prices...
Sent: BTCUSDT @ $64592.00
Sent: BTCUSDT @ $64591.50
```

---

### 🟢 Terminal 3 — Start the Stock Producer (AAPL, MSFT, GOOGL, AMZN)

```bash
venv\Scripts\activate
python producers/stock_producer.py
```

✅ Done when you see:
```
Starting stock producer...
Sent: AAPL @ $316.22
Sent: MSFT @ $384.36
```

---

### 🟢 Terminal 4 — Start PySpark (processes the data and saves to AWS)

```bash
venv\Scripts\activate
set HADOOP_HOME=C:\hadoop
python processing/stream_processor.py
```

✅ Done when you see:
```
✅ Stream processor running — writing to S3...
```

> ⏳ This takes 1-2 minutes to start the first time — that's normal!

---

### 🟢 Terminal 5 — Start the Redshift Loader (moves data to the warehouse)

```bash
venv\Scripts\activate
python processing/redshift_loader.py
```

✅ Done when you see:
```
✅ Crypto data loaded!
✅ Stock data loaded!
📊 Crypto trades: 38,388 | Stock records: 22
```

---

### 🟢 Terminal 6 — Open the Dashboard

```bash
venv\Scripts\activate
streamlit run dashboard/app.py
```

✅ Your browser will open automatically at `http://localhost:8501`

The dashboard **auto-refreshes every 30 seconds** — no need to do anything!

---

## 📊 What You'll See on the Dashboard

| Section | What It Shows |
|---|---|
| System Metrics | Total trades, latest BTC price, anomaly count |
| BTC/USDT Price Feed | Live price chart with every trade |
| Equity Price Matrix | Stock prices and volumes as bar charts |
| Anomaly Detection Log | Any price spikes flagged automatically |
| Raw Data Feed | Full table of all trades and stock records |

---

## 🛑 How to Stop Everything

Press **Ctrl + C** in each terminal to stop that process.

To stop Kafka:
```bash
docker-compose down
```

---

## 📁 Project Structure

```
market-pipeline/
├── producers/
│   ├── binance_producer.py     # Streams live BTC trades from Binance
│   └── stock_producer.py       # Polls stock prices from Alpha Vantage
├── processing/
│   ├── stream_processor.py     # PySpark streaming job → writes to S3
│   └── redshift_loader.py      # Loads S3 Parquet files into Redshift
├── dashboard/
│   └── app.py                  # Streamlit cyberpunk dashboard
├── config/
│   └── settings.py             # Shared configuration
├── docker-compose.yml          # Kafka + Zookeeper setup
├── .env                        # Your secret keys (never share this!)
└── README.md                   # This file
```

---

## 🔧 Common Problems & Fixes

**"docker: command not found"**
→ Make sure Docker Desktop is open and running before you start

**"KafkaTimeoutError: Unable to bootstrap from localhost:9092"**
→ Kafka is not running. Run `docker-compose up -d` first

**"No module named X"**
→ Make sure your virtual environment is active: `venv\Scripts\activate`

**Dashboard shows all zeros**
→ Wait for the Redshift loader to finish loading, then press **R** in the browser

**PySpark won't start on Windows**
→ Make sure `HADOOP_HOME` is set: `set HADOOP_HOME=C:\hadoop`

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data Sources | Binance WebSocket API, Alpha Vantage API |
| Message Broker | Apache Kafka 7.4.0 |
| Stream Processing | Apache PySpark 3.5.0 Structured Streaming |
| Data Lake | AWS S3 (Parquet + Snappy compression) |
| Data Warehouse | Amazon Redshift Serverless |
| Orchestration | Docker + Docker Compose |
| Dashboard | Streamlit + Plotly |
| Language | Python 3.11 |

---

## 👤 Author

**Khalid Alghanemy**
Computer Engineering Graduate — King Khalid University
IBM Data Engineering Professional Certificate | KAUST AI Bootcamp

- LinkedIn: https://linkedin.com/in/khalid-alghanemy
- GitHub: https://github.com/e2su

---

*Built from scratch as a portfolio project to demonstrate production-grade data engineering skills.*
