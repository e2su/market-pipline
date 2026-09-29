import html
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import redshift_connector
import streamlit as st

# `streamlit run dashboard/app.py` only puts dashboard/ on the import path;
# add the project root so the shared config package can be imported.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import settings  # noqa: E402

REFRESH_SECONDS = settings.DASHBOARD_REFRESH_SECONDS
STOCK_COLORS = ['#00ff8c', '#ff6b00', '#00d4ff', '#ff003c']
STOCK_VOLUME_COLORS = ['rgba(255,107,0,0.5)', 'rgba(0,255,140,0.5)',
                       'rgba(0,212,255,0.5)', 'rgba(255,0,60,0.5)']

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


# ── Redshift Connection ───────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return redshift_connector.connect(**settings.redshift_connection_args())


def _execute(query):
    cursor = get_connection().cursor()
    try:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)
    finally:
        cursor.close()


@st.cache_data(ttl=REFRESH_SECONDS - 5)
def run_query(query):
    try:
        return _execute(query)
    except (redshift_connector.InterfaceError, redshift_connector.OperationalError):
        # The cached connection went stale (network blip, Redshift idle timeout):
        # throw it away and retry once with a fresh one.
        get_connection.clear()
        return _execute(query)


# ── Helpers ───────────────────────────────────────────────────────────────────
def section_title(text):
    st.markdown(f'<div class="section-title">// {text}</div>', unsafe_allow_html=True)


def cyber_table(df):
    html_parts = ['<div style="overflow-x:auto; border:1px solid rgba(0,255,140,0.15);">',
                  '<table style="width:100%; border-collapse:collapse; font-size:0.72rem; font-family:Share Tech Mono,monospace;">',
                  '<thead><tr>']
    for col in df.columns:
        html_parts.append(f'<th style="padding:8px 12px; color:#ff6b00; letter-spacing:2px; text-transform:uppercase; border-bottom:1px solid rgba(0,255,140,0.2); text-align:left; background:#0a1628;">{html.escape(str(col))}</th>')
    html_parts.append('</tr></thead><tbody>')
    for i, row in enumerate(df.itertuples(index=False)):
        bg = 'rgba(0,255,140,0.03)' if i % 2 == 0 else '#050a0e'
        html_parts.append(f'<tr style="background:{bg};">')
        for val in row:
            # Escape values so data can never inject HTML into the page.
            html_parts.append(f'<td style="padding:6px 12px; color:#00ff8c; border-bottom:1px solid rgba(0,255,140,0.07);">{html.escape(str(val))}</td>')
        html_parts.append('</tr>')
    html_parts.append('</tbody></table></div>')
    return ''.join(html_parts)


def with_trade_time(df):
    """Replace the millisecond epoch `timestamp` column with a readable UTC time."""
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    return df


def pick_colors(palette, n):
    return [palette[i % len(palette)] for i in range(n)]


# ── Dashboard body (re-runs on its own every REFRESH_SECONDS) ─────────────────
@st.fragment(run_every=REFRESH_SECONDS)
def render_dashboard():
    st.markdown(
        f'<div class="refresh-bar">▸ LAST REFRESH: {pd.Timestamp.now().strftime("%H:%M:%S")} // AUTO-REFRESH: {REFRESH_SECONDS}s</div>',
        unsafe_allow_html=True
    )

    # ── KPI Metrics ───────────────────────────────────────────────────────────
    try:
        total_trades = run_query("SELECT COUNT(*) AS cnt FROM crypto_trades")
        latest_btc   = run_query('SELECT price FROM crypto_trades WHERE symbol=\'BTCUSDT\' ORDER BY "timestamp" DESC LIMIT 1')
        anomalies    = run_query("SELECT COUNT(*) AS cnt FROM crypto_trades WHERE anomaly='SPIKE'")
        total_stocks = run_query("SELECT COUNT(*) AS cnt FROM stock_prices")
    except Exception as e:
        st.warning(
            "▸ WAITING FOR DATA // Could not read from Redshift. "
            f"Is processing/redshift_loader.py running? ({e})"
        )
        return

    section_title("SYSTEM METRICS")
    col1, col2, col3, col4 = st.columns(4)
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

    # ── BTC Price Chart ───────────────────────────────────────────────────────
    section_title("BTC/USDT PRICE FEED")

    # Take the 1,000 *newest* trades, then sort them oldest → newest for the chart.
    btc_data = run_query("""
        SELECT "timestamp", price
        FROM crypto_trades
        WHERE symbol = 'BTCUSDT'
        ORDER BY "timestamp" DESC
        LIMIT 1000
    """)

    if not btc_data.empty:
        btc_data = with_trade_time(btc_data).sort_values('timestamp')
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
        st.plotly_chart(fig, width="stretch")

    st.divider()

    # ── Stock Charts ──────────────────────────────────────────────────────────
    section_title("EQUITY PRICE MATRIX")

    # Latest quote per symbol.
    stock_data = run_query("""
        SELECT symbol, price, volume
        FROM (
            SELECT symbol, price, volume,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY "timestamp" DESC) AS rn
            FROM stock_prices
        ) AS latest
        WHERE rn = 1
        ORDER BY symbol
    """)

    if not stock_data.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig2 = go.Figure(go.Bar(
                x=stock_data['symbol'],
                y=stock_data['price'],
                marker=dict(
                    color=pick_colors(STOCK_COLORS, len(stock_data)),
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
            st.plotly_chart(fig2, width="stretch")

        with col2:
            fig3 = go.Figure(go.Bar(
                x=stock_data['symbol'],
                y=stock_data['volume'],
                marker=dict(
                    color=pick_colors(STOCK_VOLUME_COLORS, len(stock_data)),
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
            st.plotly_chart(fig3, width="stretch")

    st.divider()

    # ── Anomaly Log ───────────────────────────────────────────────────────────
    section_title("ANOMALY DETECTION LOG")

    anomaly_data = run_query("""
        SELECT "timestamp", symbol, price, quantity, anomaly
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
        st.markdown(cyber_table(with_trade_time(anomaly_data)), unsafe_allow_html=True)

    st.divider()

    # ── Raw Data ──────────────────────────────────────────────────────────────
    section_title("RAW DATA FEED")

    tab1, tab2 = st.tabs(["▸ CRYPTO_TRADES", "▸ STOCK_PRICES"])

    with tab1:
        crypto_raw = run_query('SELECT * FROM crypto_trades ORDER BY "timestamp" DESC LIMIT 100')
        st.markdown(cyber_table(with_trade_time(crypto_raw)), unsafe_allow_html=True)

    with tab2:
        stock_raw = run_query('SELECT * FROM stock_prices ORDER BY "timestamp" DESC LIMIT 50')
        st.markdown(cyber_table(stock_raw), unsafe_allow_html=True)


render_dashboard()

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    '<div class="cyber-footer">'
    'MARKET_OS v1.0 // KAFKA + PYSPARK + AWS S3 + REDSHIFT // ALL SYSTEMS NOMINAL'
    '</div>',
    unsafe_allow_html=True
)
