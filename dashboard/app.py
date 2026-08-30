import streamlit as st
import redshift_connector
import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv
import os
import time

load_dotenv()

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MARKET_OS // DATA PIPELINE",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Orbitron:wght@400;700;900&display=swap');

    [data-testid="collapsedControl"] { display: none !important; }
    section[data-testid="stSidebar"] { display: none !important; }

    .stApp {
        background-color: #050a0e;
        background-image:
            linear-gradient(rgba(0,255,140,0.03) 1px, transparent 1px),
            linear-gradient(90deg, rgba(0,255,140,0.03) 1px, transparent 1px);
        background-size: 40px 40px;
    }

    *, p, div, span, label { font-family: 'Share Tech Mono', monospace !important; }
    h1, h2, h3 { font-family: 'Orbitron', monospace !important; }

    .cyber-header {
        font-family: 'Orbitron', monospace !important;
        font-size: 2rem;
        font-weight: 900;
        color: #00ff8c;
        text-shadow: 0 0 20px #00ff8c, 0 0 40px rgba(0,255,140,0.3);
        letter-spacing: 6px;
        text-transform: uppercase;
        border-bottom: 1px solid rgba(0,255,140,0.27);
        padding-bottom: 10px;
        margin-bottom: 5px;
    }

    .cyber-subtitle {
        color: #ff6b00;
        font-size: 0.7rem;
        letter-spacing: 4px;
        text-transform: uppercase;
        margin-bottom: 25px;
    }

    .section-title {
        font-family: 'Orbitron', monospace !important;
        color: #ff6b00;
        font-size: 0.8rem;
        letter-spacing: 4px;
        text-transform: uppercase;
        border-left: 3px solid #ff6b00;
        padding-left: 10px;
        margin: 20px 0 10px 0;
        text-shadow: 0 0 10px rgba(255,107,0,0.5);
    }

    [data-testid="metric-container"] {
        background: linear-gradient(135deg, #0a1628 0%, #0d1f0d 100%);
        border: 1px solid rgba(0,255,140,0.2);
        border-left: 3px solid #00ff8c;
        padding: 15px !important;
        box-shadow: 0 0 20px rgba(0,255,140,0.05);
    }

    [data-testid="stMetricLabel"] > div {
        color: #ff6b00 !important;
        font-size: 0.65rem !important;
        letter-spacing: 3px !important;
        text-transform: uppercase !important;
    }

    [data-testid="stMetricValue"] > div {
        color: #00ff8c !important;
        font-size: 1.6rem !important;
        text-shadow: 0 0 10px rgba(0,255,140,0.5) !important;
    }

    hr {
        border: none !important;
        border-top: 1px solid rgba(0,255,140,0.1) !important;
        margin: 15px 0 !important;
    }

    .stTabs [data-baseweb="tab-list"] {
        background: #0a1628 !important;
        border-bottom: 1px solid rgba(0,255,140,0.2) !important;
        gap: 5px;
    }

    .stTabs [data-baseweb="tab"] {
        color: rgba(0,255,140,0.4) !important;
        letter-spacing: 2px !important;
        font-size: 0.7rem !important;
        background: transparent !important;
    }

    .stTabs [aria-selected="true"] {
        color: #00ff8c !important;
        border-bottom: 2px solid #00ff8c !important;
        background: rgba(0,255,140,0.05) !important;
    }

    ::-webkit-scrollbar { width: 3px; height: 3px; }
    ::-webkit-scrollbar-track { background: #050a0e; }
    ::-webkit-scrollbar-thumb { background: rgba(0,255,140,0.3); }

    .cyber-footer {
        color: rgba(0,255,140,0.13);
        font-size: 0.6rem;
        letter-spacing: 3px;
        text-align: center;
        margin-top: 20px;
    }

    .refresh-bar {
        background: #0a1628;
        border: 1px solid rgba(0,255,140,0.15);
        padding: 5px 12px;
        font-size: 0.65rem;
        color: rgba(0,255,140,0.5);
        letter-spacing: 2px;
        text-align: right;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="cyber-header">⬡ MARKET_OS // PIPELINE DASHBOARD</div>', unsafe_allow_html=True)
st.markdown('<div class="cyber-subtitle">▸ KAFKA + PYSPARK + AWS REDSHIFT ▸ REAL-TIME MARKET INTELLIGENCE ▸ SYS_ONLINE</div>', unsafe_allow_html=True)

# ── Refresh bar ───────────────────────────────────────────────────────────────
st.markdown(
    f'<div class="refresh-bar">▸ LAST REFRESH: {pd.Timestamp.now().strftime("%H:%M:%S")} // AUTO-REFRESH: 30s</div>',
    unsafe_allow_html=True
)

# ── Redshift Connection ───────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return redshift_connector.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT")),
        database=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        ssl=True,
        sslmode="require"
    )

@st.cache_data(ttl=25)
def run_query(query):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)

# ── KPI Metrics ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">// SYSTEM METRICS</div>', unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)

total_trades = run_query("SELECT COUNT(*) as cnt FROM crypto_trades")
latest_btc   = run_query('SELECT price FROM crypto_trades WHERE symbol=\'BTCUSDT\' ORDER BY "timestamp" DESC LIMIT 1')
anomalies    = run_query("SELECT COUNT(*) as cnt FROM crypto_trades WHERE anomaly='SPIKE'")
total_stocks = run_query("SELECT COUNT(*) as cnt FROM stock_prices")

with col1:
    st.metric("TOTAL BTC TRADES", f"{int(total_trades['cnt'][0]):,}")
with col2:
    price = float(latest_btc['price'][0]) if len(latest_btc) > 0 else 0
    st.metric("LATEST BTC PRICE", f"${price:,.2f}")
with col3:
    st.metric("PRICE ANOMALIES", f"{int(anomalies['cnt'][0]):,}")
with col4:
    st.metric("STOCK RECORDS", f"{int(total_stocks['cnt'][0]):,}")

st.divider()

# ── BTC Price Chart ───────────────────────────────────────────────────────────
st.markdown('<div class="section-title">// BTC/USDT PRICE FEED</div>', unsafe_allow_html=True)

btc_data = run_query("""
    SELECT "timestamp", price
    FROM crypto_trades
    WHERE symbol = 'BTCUSDT'
    ORDER BY "timestamp" ASC
    LIMIT 1000
""")

if not btc_data.empty:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=btc_data['timestamp'],
        y=btc_data['price'],
        mode='lines',
        line=dict(color='#00ff8c', width=1.5),
        fill='tozeroy',
        fillcolor='rgba(0,255,140,0.05)',
        name='BTC/USDT'
    ))
    fig.update_layout(
        plot_bgcolor='#050a0e',
        paper_bgcolor='#050a0e',
        font=dict(color='#00ff8c', family='Share Tech Mono'),
        xaxis=dict(
            gridcolor='rgba(0,255,140,0.07)',
            color='rgba(0,255,140,0.4)',
            showline=True,
            linecolor='rgba(0,255,140,0.2)',
            title=''
        ),
        yaxis=dict(
            gridcolor='rgba(0,255,140,0.07)',
            color='rgba(0,255,140,0.4)',
            showline=True,
            linecolor='rgba(0,255,140,0.2)',
            tickprefix='$',
            title='',
            range=[
                btc_data['price'].min() * 0.9999,
                btc_data['price'].max() * 1.0001
            ]
        ),
        margin=dict(l=10, r=10, t=10, b=10),
        height=300,
        showlegend=False,
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Stock Charts ──────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">// EQUITY PRICE MATRIX</div>', unsafe_allow_html=True)

stock_data = run_query("""
    SELECT symbol, price, volume
    FROM stock_prices
    ORDER BY "timestamp" DESC
    LIMIT 20
""")

if not stock_data.empty:
    unique_stocks = stock_data.drop_duplicates('symbol')
    col1, col2 = st.columns(2)

    with col1:
        fig2 = go.Figure(go.Bar(
            x=unique_stocks['symbol'],
            y=unique_stocks['price'],
            marker=dict(
                color=['#00ff8c', '#ff6b00', '#00d4ff', '#ff003c'],
                line=dict(color='#050a0e', width=1)
            ),
            hovertemplate='%{x}: $%{y:,.2f}<extra></extra>'
        ))
        fig2.update_layout(
            plot_bgcolor='#050a0e',
            paper_bgcolor='#050a0e',
            font=dict(color='#00ff8c', family='Share Tech Mono'),
            xaxis=dict(gridcolor='rgba(255,255,255,0.05)', color='rgba(0,255,140,0.5)'),
            yaxis=dict(gridcolor='rgba(0,255,140,0.07)', color='rgba(0,255,140,0.5)', tickprefix='$'),
            margin=dict(l=10, r=10, t=35, b=10),
            height=280,
            title=dict(text='PRICE // USD', font=dict(color='#ff6b00', size=10), x=0)
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        fig3 = go.Figure(go.Bar(
            x=unique_stocks['symbol'],
            y=unique_stocks['volume'],
            marker=dict(
                color=['rgba(255,107,0,0.5)', 'rgba(0,255,140,0.5)',
                       'rgba(0,212,255,0.5)', 'rgba(255,0,60,0.5)'],
                line=dict(color='#ff6b00', width=1)
            ),
            hovertemplate='%{x}: %{y:,}<extra></extra>'
        ))
        fig3.update_layout(
            plot_bgcolor='#050a0e',
            paper_bgcolor='#050a0e',
            font=dict(color='#ff6b00', family='Share Tech Mono'),
            xaxis=dict(gridcolor='rgba(255,255,255,0.05)', color='rgba(255,107,0,0.5)'),
            yaxis=dict(gridcolor='rgba(255,107,0,0.07)', color='rgba(255,107,0,0.5)'),
            margin=dict(l=10, r=10, t=35, b=10),
            height=280,
            title=dict(text='VOLUME // SHARES', font=dict(color='#ff6b00', size=10), x=0)
        )
        st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── Anomaly Log ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">// ANOMALY DETECTION LOG</div>', unsafe_allow_html=True)

anomaly_data = run_query("""
    SELECT symbol, price, quantity, anomaly
    FROM crypto_trades
    WHERE anomaly = 'SPIKE'
    ORDER BY "timestamp" DESC
    LIMIT 50
""")

if anomaly_data.empty:
    st.markdown(
        '<div style="color:rgba(0,255,140,0.5); font-size:0.75rem; letter-spacing:2px; '
        'padding:10px; border:1px solid rgba(0,255,140,0.15);">'
        '▸ STATUS: ALL CLEAR // NO ANOMALIES DETECTED // PRICES WITHIN NORMAL RANGE'
        '</div>',
        unsafe_allow_html=True
    )
else:
    st.markdown(cyber_table(anomaly_data), unsafe_allow_html=True)

st.divider()

# ── Raw Data ──────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">// RAW DATA FEED</div>', unsafe_allow_html=True)

def cyber_table(df):
    html = '<div style="overflow-x:auto; border:1px solid rgba(0,255,140,0.15);">'
    html += '<table style="width:100%; border-collapse:collapse; font-size:0.72rem; font-family:Share Tech Mono,monospace;">'
    html += '<thead><tr>'
    for col in df.columns:
        html += f'<th style="padding:8px 12px; color:#ff6b00; letter-spacing:2px; text-transform:uppercase; border-bottom:1px solid rgba(0,255,140,0.2); text-align:left; background:#0a1628;">{col}</th>'
    html += '</tr></thead><tbody>'
    for i, row in df.iterrows():
        bg = 'rgba(0,255,140,0.03)' if i % 2 == 0 else '#050a0e'
        html += f'<tr style="background:{bg};">'
        for val in row:
            html += f'<td style="padding:6px 12px; color:#00ff8c; border-bottom:1px solid rgba(0,255,140,0.07);">{val}</td>'
        html += '</tr>'
    html += '</tbody></table></div>'
    return html

tab1, tab2 = st.tabs(["▸ CRYPTO_TRADES", "▸ STOCK_PRICES"])

with tab1:
    crypto_raw = run_query('SELECT * FROM crypto_trades ORDER BY "timestamp" DESC LIMIT 100')
    st.markdown(cyber_table(crypto_raw), unsafe_allow_html=True)

with tab2:
    stock_raw = run_query('SELECT * FROM stock_prices ORDER BY "timestamp" DESC LIMIT 50')
    st.markdown(cyber_table(stock_raw), unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    '<div class="cyber-footer">'
    'MARKET_OS v1.0 // KAFKA + PYSPARK + AWS S3 + REDSHIFT // ALL SYSTEMS NOMINAL'
    '</div>',
    unsafe_allow_html=True
)

# ── Auto Refresh ──────────────────────────────────────────────────────────────
time.sleep(5)
st.rerun()