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
2. **Grabs** stock prices (AAPL, MSFT, GOOGL, AMZN) from Alpha Vantage (every 4 hours on the free tier)
3. **Processes** the data using Apache Kafka and PySpark, flagging sudden price spikes
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
| Python 3.11 or 3.12 | Runs the code | https://www.python.org/downloads/ |
| Docker Desktop | Runs Kafka locally | https://www.docker.com/products/docker-desktop/ |
| VS Code | Code editor | https://code.visualstudio.com/ |
| Java 17 | Required by PySpark 3.5 (Java 8/11/17 are supported) | https://adoptium.net/ |

You also need accounts on:
- **Alpha Vantage** (free) → https://www.alphavantage.co/support/#api-key
- **AWS** (free tier) → https://aws.amazon.com/free/

---

## ⚙️ First Time Setup (Do This Once)

### Step 1 — Download the project
```bash
git clone https://github.com/e2su/market-pipline.git
cd market-pipline
```

### Step 2 — Create a virtual environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux
```

### Step 3 — Install all dependencies
```bash
pip install -r requirements.txt
```

> ⚠️ PySpark must stay on **3.5.x**: the Kafka connector the stream processor downloads is built for Spark 3.5.
> If you also installed Spark separately and set `SPARK_HOME`, the stream processor ignores it and uses the Spark bundled with PySpark.

### Step 4 — Set up your secret keys

Copy the template and fill in your own values:

```bash
copy .env.example .env       # Windows
cp .env.example .env         # Mac/Linux
```

`.env` is listed in `.gitignore`, so git will never commit it. **Never commit real keys** — if one ever gets pushed, rotate it immediately.

> 💡 **Where to get these keys:**
> - Alpha Vantage key → sign up at https://www.alphavantage.co/support/#api-key
> - AWS keys → AWS Console → IAM → Users → Security Credentials
> - S3 bucket name → AWS Console → S3
> - Redshift endpoint → AWS Console → Redshift Serverless → your workgroup
> - `REDSHIFT_IAM_ROLE` (recommended) → create an IAM role that can read your bucket, attach it to your Redshift namespace, and paste its ARN. The loader then uses the role instead of sending your access keys to Redshift.

### Step 5 — Windows only: Set up Hadoop (needed by PySpark)
```bash
# Download winutils.exe and hadoop.dll from:
# https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.2/bin
# Place both files in C:\hadoop\bin\

setx HADOOP_HOME "C:\hadoop" /M    # Run as Administrator
```

---

## 🚀 How to Run the Project

You need **6 terminals** open at the same time: one for Kafka and one for each of the five Python processes. Think of each terminal as a worker doing one job.

> 💡 **Tip:** In VS Code, press **Ctrl + `** to open a terminal, and **Ctrl + Shift + `** to open another one.

> 💡 Run every command from the **project root folder**. The scripts are started with `python -m ...` so they can import the shared `config` package.

---

### 🟢 Terminal 1 — Start Kafka (the message broker)

```bash
docker-compose up -d
```

✅ Done when you see: `Container kafka Healthy` (the `kafka-init` container then creates the two topics and exits).

Check it's running:
```bash
docker ps
```
You should see both `kafka` and `zookeeper` listed.

---

### 🟢 Terminal 2 — Start the Binance Producer (live BTC prices)

```bash
venv\Scripts\activate
python -m producers.binance_producer
```

✅ Done when you see:
```
Connected to Binance! Streaming BTC prices...
Sent 100 trades — latest BTCUSDT @ $64592.01
```

It reconnects on its own if the connection drops (Binance closes every connection after 24 hours).

---

### 🟢 Terminal 3 — Start the Stock Producer (AAPL, MSFT, GOOGL, AMZN)

```bash
venv\Scripts\activate
python -m producers.stock_producer
```

✅ Done when you see:
```
Starting stock producer for AAPL, MSFT, GOOGL, AMZN (every 14400s)...
Sent: AAPL @ $316.22
Sent: MSFT @ $384.36
```

> ℹ️ Alpha Vantage's free tier allows **25 requests per day**, so by default the 4 symbols are polled every 4 hours. Change `STOCK_POLL_INTERVAL_SECONDS` in `.env` if you have a paid key.

---

### 🟢 Terminal 4 — Start PySpark (processes the data and saves to AWS)

```bash
venv\Scripts\activate
python -m processing.stream_processor
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
python -m processing.redshift_loader
```

✅ Done when you see:
```
📦 crypto_trades: 12 new file(s)
📦 stock_prices: 1 new file(s)
📊 Crypto trades: 38,388 | Stock records: 22
```

Every minute it loads only the S3 files it hasn't loaded yet (it keeps track in a `loaded_files` table).

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

## 🔍 How Anomaly Detection Works

Every 30 seconds Spark processes a micro-batch of BTC trades. Each trade is compared with the **median price of the previous batch**; if it is more than `ANOMALY_THRESHOLD_PCT` percent away (default **0.5%**), it is flagged as a `SPIKE`. This catches sudden jumps while ignoring the normal drift of the price.

---

## 🛑 How to Stop Everything

Press **Ctrl + C** in each terminal to stop that process.

To stop Kafka:
```bash
docker-compose down
```

---

## 🧪 Running the Tests

```bash
pytest
```

The Spark tests start a small local Spark session, so Java 17 must be installed. GitHub Actions runs the same tests on every push.

---

## 📁 Project Structure

```
market-pipeline/
├── producers/
│   ├── binance_producer.py     # Streams live BTC trades from Binance
│   └── stock_producer.py       # Polls stock prices from Alpha Vantage
├── processing/
│   ├── stream_processor.py     # PySpark streaming job + anomaly detection → writes to S3
│   └── redshift_loader.py      # Loads new S3 Parquet files into Redshift
├── dashboard/
│   └── app.py                  # Streamlit cyberpunk dashboard
├── config/
│   └── settings.py             # Shared configuration (reads .env)
├── tests/                      # pytest tests
├── docker-compose.yml          # Kafka + Zookeeper setup
├── requirements.txt            # Python dependencies
├── .env.example                # Template for your .env (safe to commit)
├── .env                        # Your secret keys (git-ignored — never share this!)
└── README.md                   # This file
```

---

## 🔧 Common Problems & Fixes

**"docker: command not found"**
→ Make sure Docker Desktop is open and running before you start

**"KafkaTimeoutError: Unable to bootstrap from localhost:9092"**
→ Kafka is not running. Run `docker-compose up -d` first

**"No module named X"**
→ Make sure your virtual environment is active (`venv\Scripts\activate`) and you ran `pip install -r requirements.txt`

**"No module named config"**
→ Run the scripts from the project root with `python -m ...`, as shown above

**`NoSuchMethodError: scala.Predef$.wrapRefArray`**
→ Spark version mismatch. Run `pip install -r requirements.txt` to get PySpark 3.5.x

**Dashboard says "WAITING FOR DATA"**
→ Start the Redshift loader and wait for its first run to finish

**Stock producer says "Rate limited by Alpha Vantage"**
→ The free daily quota (25 requests) is used up; it will try again at the next poll

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
| Language | Python 3.11 / 3.12 |

---

## 👤 Author

**Khalid Alghanemy**
Computer Engineering Graduate — King Khalid University
IBM Data Engineering Professional Certificate | KAUST AI Bootcamp

- LinkedIn: https://linkedin.com/in/khalid-alghanemy
- GitHub: https://github.com/e2su

---

*Built from scratch as a portfolio project to demonstrate production-grade data engineering skills.*
