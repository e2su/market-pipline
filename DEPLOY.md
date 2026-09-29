# Running the pipeline 24/7 on AWS EC2

This guide puts the whole pipeline on one small server that keeps running when your laptop is off.
Everything runs in Docker: Kafka, Postgres, the two producers, the Spark job, the loader and the dashboard.
Every container restarts on its own after a crash or a reboot.

```
EC2 server (eu-north-1)
├── zookeeper + kafka         message broker (not reachable from the internet)
├── binance-producer          live BTC trades   → Kafka
├── stock-producer            stock quotes      → Kafka
├── stream-processor          Kafka → Spark → S3 (Parquet)
├── loader                    S3 → Postgres
├── postgres                  the warehouse (not reachable from the internet)
└── dashboard                 Streamlit on port 8501
```

**Why Postgres instead of Redshift?** Redshift Serverless bills while queries run. A dashboard refreshing every
30 seconds and a loader running every minute keep it busy nearly all day, which can cost over $1,000 a month.
Postgres on the same server costs nothing extra and runs the same SQL. The Redshift code is still in the repo:
set `WAREHOUSE=redshift` to use it locally.

**Cost:** roughly $60–70 a month for a `t3.large` plus about $3 for 30 GB of disk and a few dollars for the
public IP address. Check the [EC2 pricing page](https://aws.amazon.com/ec2/pricing/on-demand/) for current prices.

---

## Before you start

- [ ] Your leaked keys are rotated and pull request #1 is merged into `main`.
- [ ] You have an S3 bucket in `eu-north-1` (you already do).
- [ ] **Set a budget alarm:** AWS Console → Billing → Budgets → Create budget → "Monthly cost budget", e.g. $80,
      with an email alert. You'll hear about surprises before they get expensive.

---

## Step 1 — Create an IAM role for the server

Instead of copying your AWS keys onto the server, give the server its own **role**. AWS hands it short-lived
credentials automatically, so there's no key to leak.

1. AWS Console → **IAM → Roles → Create role**.
2. Trusted entity: **AWS service**, use case: **EC2**. Click Next.
3. Skip the permission list (click Next), name the role `market-pipeline-ec2`, and create it.
4. Open the role → **Add permissions → Create inline policy → JSON**, paste this (replace `YOUR_BUCKET`), and save it as `s3-market-bucket`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::YOUR_BUCKET" },
    { "Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], "Resource": "arn:aws:s3:::YOUR_BUCKET/*" }
  ]
}
```

This role can only touch your one bucket. That's the **principle of least privilege**: give each thing only the access it needs.

---

## Step 2 — Launch the server

AWS Console → switch the region (top right) to **Europe (Stockholm) eu-north-1** → **EC2 → Launch instance**:

| Setting | Value | Why |
|---|---|---|
| Name | `market-pipeline` | |
| Image | Ubuntu Server 24.04 LTS | Popular, well documented |
| Instance type | `t3.large` (2 vCPU, 8 GB RAM) | Spark and Kafka need the memory |
| Key pair | Create new → download the `.pem` file | Your SSH login. Keep it safe |
| Network → Security group | Allow SSH (22) from **My IP**; add a rule: Custom TCP **8501** from **My IP** | Only you can reach it. Use "Anywhere" for 8501 if you want to share the dashboard |
| Storage | 30 GB gp3 | Docker images + a day of Kafka data |
| Advanced → IAM instance profile | `market-pipeline-ec2` | The role from Step 1 |

Click **Launch instance**. Then, so the address doesn't change when you restart the server:
**EC2 → Elastic IPs → Allocate → Associate** it with the instance. Note the IP address.

> ⚠️ Stay in `eu-north-1` (or another non-US region): Binance blocks connections from US-region servers.

---

## Step 3 — Log in and install Docker

From your computer (PowerShell works):

```bash
ssh -i path\to\your-key.pem ubuntu@YOUR_ELASTIC_IP
```

On the server:

```bash
curl -fsSL https://get.docker.com | sudo sh     # installs Docker + the compose plugin, starts on boot
sudo usermod -aG docker ubuntu                   # lets you run docker without sudo
exit                                             # log out and back in so that takes effect
```

---

## Step 4 — Get the code and configure it

```bash
ssh -i path\to\your-key.pem ubuntu@YOUR_ELASTIC_IP
git clone https://github.com/e2su/market-pipline.git market-pipeline
cd market-pipline
cp .env.example .env
nano .env
```

> If the repo is private, `git clone` asks for a password: use a GitHub
> [personal access token](https://github.com/settings/tokens) (read-only "Contents" access to this repo) instead of your password.

In `.env` fill in:

| Variable | Value |
|---|---|
| `ALPHA_VANTAGE_API_KEY` | your (new) key |
| `S3_BUCKET_NAME` | your bucket |
| `AWS_REGION` | `eu-north-1` |
| `POSTGRES_PASSWORD` | a long random password — generate one with `openssl rand -base64 24` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | **leave empty**: the IAM role provides credentials |
| Redshift variables | leave empty |

Save with **Ctrl+O, Enter**, exit with **Ctrl+X**. Then lock the file so only you can read it:

```bash
chmod 600 .env
```

---

## Step 5 — Start everything

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

The first build takes about 5 minutes. Then check that everything is up:

```bash
docker compose -f docker-compose.prod.yml ps
```

Every service should say `running` (or `healthy`); `kafka-init` shows `exited (0)`, which is correct: it only creates the topics.

Watch a service's logs (Ctrl+C stops watching, not the service):

```bash
docker compose -f docker-compose.prod.yml logs -f stream-processor
docker compose -f docker-compose.prod.yml logs -f loader
```

The first data reaches the dashboard about 2 minutes after the stream processor starts.

Open **http://YOUR_ELASTIC_IP:8501** in your browser. 🎉

---

## Step 6 — Prove it survives a reboot

```bash
sudo reboot
```

Wait a minute, SSH back in, and run the `ps` command again. Everything should be back without you doing anything.
That's `restart: unless-stopped` in `docker-compose.prod.yml` plus Docker starting on boot.

---

## Day-to-day

| Task | Command (run inside `~/market-pipeline`) |
|---|---|
| Status | `docker compose -f docker-compose.prod.yml ps` |
| Logs | `docker compose -f docker-compose.prod.yml logs -f <service>` |
| Memory / CPU use | `docker stats` |
| Deploy new code | `git pull && docker compose -f docker-compose.prod.yml up -d --build` |
| Stop everything | `docker compose -f docker-compose.prod.yml down` (data in Postgres and S3 is kept) |
| Stop paying | Stop the instance in the EC2 console (you still pay for the disk and Elastic IP), or terminate it and release the Elastic IP |

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `binance-producer` logs HTTP 451 | The server is in a US region. Launch it in `eu-north-1` |
| `stream-processor` or `loader` logs `AccessDenied` / `NoCredentialsError` | The IAM role isn't attached (Step 2) or its policy has the wrong bucket name (Step 1) |
| Containers restarting, `docker stats` near 8 GB | Out of memory. Use a bigger instance (`t3.xlarge`) |
| Stock prices never appear | Alpha Vantage's free quota (25 requests/day) is used up. The producer retries every 4 hours |
| Dashboard page doesn't load | Port 8501 isn't open to your IP in the security group, or your IP changed |
| `POSTGRES_PASSWORD is missing a value` | Set it in `.env` (Step 4) |
