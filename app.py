"""
Navedas Governance Intelligence Platform
Real-Time AI Order Governance Engine — US/Shopify Aligned
Streamlit Cloud Deployable
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from streamlit_autorefresh import st_autorefresh
import os
from pipeline import (
    load_data, compute_kpis, compute_time_series,
    compute_agent_stats, generate_live_order,
    load_csv_to_db, load_from_db, db_exists, _DB_FILE
)
import sqlite3

try:
    from governance_engine import compute_governance_health_score
except Exception:
    def compute_governance_health_score(recovery_rate=0, residual_loss=0, ai_loss=0,
                                        sla_compliance=0, successful_interventions=0,
                                        total_interventions=0):
        score = min(100.0, max(0.0, recovery_rate * 100))
        band  = "Excellent" if score >= 90 else ("Healthy" if score >= 75 else ("Warning" if score >= 60 else "Critical"))
        color = {"Excellent":"#059669","Healthy":"#2563eb","Warning":"#d97706","Critical":"#e11d48"}[band]
        return {"score": round(score,1), "band": band, "color": color}

try:
    from navedas_agent import (run_agent_cycle, get_agent_summary,
                               get_recent_events, get_feed_pending_count)
except Exception:
    def run_agent_cycle(db_path=None): return {"processed":0,"recovered":0,"revenue_prevented":0.0}
    def get_agent_summary(db_path=None): return {"total":0,"recovered":0,"rev_prevented":0.0,"margin_saved":0.0,"int_cost":0.0,"net_profit":0.0}
    def get_recent_events(db_path=None, limit=20): return []
    def get_feed_pending_count(db_path=None): return 0

try:
    from synthetic_feed_generator import (ensure_schema, generate_order, insert_orders_batch)
except Exception:
    def ensure_schema(conn): pass
    def generate_order(counter): return {}
    def insert_orders_batch(conn, orders): pass

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Navedas Governance Intelligence Platform",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Password Protection ────────────────────────────────────────────────────────
def _check_password() -> bool:
    if st.session_state.get("_authenticated"):
        return True

    allowed_raw = st.secrets.get("ALLOWED_IPS", os.environ.get("ALLOWED_IPS", ""))
    if allowed_raw.strip():
        allowed_ips = [ip.strip() for ip in allowed_raw.split(",") if ip.strip()]
        visitor_ip  = st.context.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        if visitor_ip and visitor_ip not in allowed_ips:
            st.error("🚫 Access denied. Your IP is not authorised.")
            st.stop()

    st.markdown("""
    <div style='display:flex; justify-content:center; align-items:center;
                min-height:80vh; flex-direction:column;'>
      <div style='background:#ffffff; border:2px solid #6C63FF; border-radius:20px;
                  padding:48px 56px; max-width:420px; width:100%; text-align:center;
                  box-shadow: 0 20px 60px rgba(108,99,255,0.12);'>
        <div style='font-size:52px; margin-bottom:12px;'>🏛️</div>
        <h2 style='color:#1e293b; margin:0 0 4px;'>Navedas GIP</h2>
        <p style='color:#64748b; font-size:13px; margin-bottom:32px;'>
          Governance Intelligence Platform<br>Restricted Access
        </p>
      </div>
    </div>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 1.2, 1])
    with col:
        with st.form("login_form"):
            pwd = st.text_input("Password", type="password", placeholder="Enter access password")
            login = st.form_submit_button("🔐 Access Dashboard", use_container_width=True, type="primary")
        if login:
            correct = st.secrets.get("APP_PASSWORD", os.environ.get("APP_PASSWORD", "Navedas@2024"))
            if pwd == correct:
                st.session_state["_authenticated"] = True
                st.rerun()
            else:
                st.error("Incorrect password. Please try again.")
    return False

if not _check_password():
    st.stop()

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
  html, body, [class*="css"], * { font-family: 'Inter', sans-serif !important; }

  /* ── Base ── */
  .stApp { background: #F6F8FC !important; }
  .main  { background: #F6F8FC !important; }
  section[data-testid="stSidebar"] {
    background: #ffffff !important;
    border-right: 2px solid #ede9fe !important;
  }
  section[data-testid="stSidebar"] .stMarkdown p,
  section[data-testid="stSidebar"] label,
  section[data-testid="stSidebar"] span { color: #374151 !important; }

  /* ── Tabs ── */
  .stTabs [data-baseweb="tab-list"] {
    background: #ffffff !important;
    border: 1px solid #e5e7eb !important;
    border-radius: 12px !important;
    padding: 5px !important;
    gap: 4px !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06) !important;
  }
  .stTabs [data-baseweb="tab"] {
    background: transparent !important;
    border-radius: 8px !important;
    color: #6b7280 !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    padding: 8px 18px !important;
    border: none !important;
  }
  .stTabs [aria-selected="true"] {
    background: #6C63FF !important;
    color: #ffffff !important;
    font-weight: 700 !important;
    border-radius: 8px !important;
  }
  .stTabs [data-baseweb="tab-highlight"] { display: none !important; }
  .stTabs [data-baseweb="tab-border"]    { display: none !important; }

  /* ── Buttons ── */
  button[kind="primary"], .stButton > button[data-testid*="primary"] {
    background: #6C63FF !important; border-color: #6C63FF !important;
    color: white !important; border-radius: 8px !important; font-weight: 600 !important;
  }
  .stButton > button {
    border-radius: 8px !important;
    border: 1px solid #e5e7eb !important;
    color: #374151 !important;
    font-weight: 500 !important;
  }

  /* ── Dataframe ── */
  .stDataFrame { border-radius: 12px !important; border: 1px solid #e5e7eb !important; overflow: hidden !important; }
  [data-testid="stDataFrame"] { background: white !important; border-radius: 12px !important; }

  /* ── Expanders ── */
  [data-testid="stExpander"] {
    background: #ffffff !important;
    border: 1px solid #e5e7eb !important;
    border-radius: 10px !important;
  }

  /* ── Form ── */
  [data-testid="stForm"] {
    background: #ffffff !important;
    border: 1px solid #e5e7eb !important;
    border-radius: 14px !important;
    padding: 20px !important;
  }

  /* ── Section headers ── */
  .section-header {
    font-size: 11px !important; font-weight: 700 !important;
    text-transform: uppercase !important; letter-spacing: .09em !important;
    color: #6C63FF !important; margin-bottom: 14px !important;
    padding-bottom: 8px !important; border-bottom: 2px solid #e8e6ff !important;
  }

  /* ── Plotly chart cards ── */
  [data-testid="stPlotlyChart"] {
    background: #ffffff !important;
    border: 1px solid #e5e7eb !important;
    border-radius: 14px !important;
    padding: 12px 8px 4px 8px !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04) !important;
    margin-bottom: 4px !important;
  }

  /* ── KPI cards shadow ── */
  .kpi-card { box-shadow: 0 2px 8px rgba(0,0,0,0.05); }

  hr { border-color: #e5e7eb !important; }

  .live-badge {
    display: inline-block; width: 8px; height: 8px; border-radius: 50%;
    background: #10b981; animation: pulse 1.5s infinite; margin-right: 6px;
  }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }

  footer { visibility: hidden; }
  #MainMenu { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

CHART_LAYOUT = dict(
    paper_bgcolor='rgba(255,255,255,0)', plot_bgcolor='rgba(255,255,255,1)',
    font=dict(color='#475569', family='Inter'),
    margin=dict(l=40, r=20, t=30, b=40),
    xaxis=dict(gridcolor='#e2e8f0', showgrid=True, color='#64748b'),
    yaxis=dict(gridcolor='#e2e8f0', showgrid=True, color='#64748b'),
)
BASE_LAYOUT = dict(
    paper_bgcolor='rgba(255,255,255,0)', plot_bgcolor='rgba(255,255,255,1)',
    font=dict(color='#475569', family='Inter'),
)

# ── Session state init ─────────────────────────────────────────────────────────
if 'df' not in st.session_state:           st.session_state.df = None
if 'db_path' not in st.session_state:
    if db_exists(_DB_FILE):
        st.session_state.db_path = _DB_FILE
        st.session_state.df = load_from_db(_DB_FILE)
    else:
        _bundled = os.path.join(os.path.dirname(__file__), 'data.csv')
        if os.path.exists(_bundled):
            with open(_bundled, 'r', encoding='utf-8') as _f:
                _content = _f.read()
            _db = load_csv_to_db(_content)
            st.session_state.db_path = _db
            st.session_state.df = load_from_db(_db)
        else:
            st.session_state.db_path = None
if 'live_orders' not in st.session_state:  st.session_state.live_orders = []
if 'sim_running' not in st.session_state:  st.session_state.sim_running = False
if 'live_counter' not in st.session_state: st.session_state.live_counter = 800000
if 'feed_counter' not in st.session_state: st.session_state.feed_counter = 0
_LIVE_DEFAULTS = {
    'count': 0, 'rev_prevented': 0, 'margin_saved': 0,
    'int_cost': 0, 'net_profit': 0, 'residual_loss': 0,
    'ai_cancelled': 0, 'recoverable': 0, 'not_recoverable': 0,
    'recovered': 0, 'auto_recoveries': 0, 'human_recoveries': 0,
}
if 'live_stats' not in st.session_state:
    st.session_state.live_stats = dict(_LIVE_DEFAULTS)
else:
    for _k, _v in _LIVE_DEFAULTS.items():
        if _k not in st.session_state.live_stats:
            st.session_state.live_stats[_k] = _v

# ── Auto-refresh ───────────────────────────────────────────────────────────────
if st.session_state.sim_running:
    st_autorefresh(interval=2000, key="live_refresh")

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
<div style='text-align:center;padding:16px 0 8px;'>
  <div style='font-size:32px;'>🏛️</div>
  <div style='font-size:15px;font-weight:800;color:#1e293b;margin-top:4px;'>Navedas GIP</div>
  <div style='font-size:11px;color:#6b7280;margin-top:2px;'>Governance Intelligence Platform</div>
</div>
""", unsafe_allow_html=True)
    st.markdown("---")

    # ── Data Source ─────────────────────────────────────────────────────────
    st.markdown("#### 📂 Data Source")
    if st.session_state.df is not None:
        st.success(f"✅ {len(st.session_state.df):,} orders loaded from DB")

    show_upload = st.checkbox("Replace data (optional)", value=False)
    if show_upload:
        uploaded = st.file_uploader("Upload new CSV", type=['csv'],
                                    help="Replaces the current DB with fresh data")
        if uploaded:
            with st.spinner("Loading CSV into database…"):
                content = uploaded.read().decode('utf-8')
                db_path = load_csv_to_db(content)
                st.session_state.db_path = db_path
                st.session_state.df = load_from_db(db_path)
            st.success(f"✅ {len(st.session_state.df):,} orders loaded")

    st.markdown("---")

    # ── Agent Controls ──────────────────────────────────────────────────────
    st.markdown("#### ⚡ Live Simulation")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("▶ Start", use_container_width=True, type="primary"):
            st.session_state.sim_running = True
    with col2:
        if st.button("⏹ Stop", use_container_width=True):
            st.session_state.sim_running = False

    if st.session_state.sim_running:
        st.markdown('<span class="live-badge"></span> **Live feed active**', unsafe_allow_html=True)
        ls = st.session_state.live_stats
        for _ in range(3):
            st.session_state.live_counter += 1
            o = generate_live_order(st.session_state.live_counter)
            st.session_state.live_orders.insert(0, o)
            ls['count']           += 1
            ls['rev_prevented']   += o['_rev_prevented']
            ls['margin_saved']    += o['_margin_saved']
            ls['int_cost']        += o['_int_cost']
            ls['net_profit']      += o['_net_profit']
            ls['residual_loss']   += o['_residual_loss']
            ls['ai_cancelled']    += o['_ai_cancelled']
            ls['recoverable']     += o['_recoverable']
            ls['not_recoverable'] += o['_not_recoverable']
            ls['recovered']       += o['_recovered']
            ls['auto_recoveries'] += o['_auto_recovery']
            ls['human_recoveries']+= o['_human_recovery']
        st.session_state.live_orders = st.session_state.live_orders[:50]

    st.markdown("---")

    # ── Governance Agent Controls ───────────────────────────────────────────
    st.markdown("#### 🤖 Governance Agent")
    pending = get_feed_pending_count(_DB_FILE)
    st.markdown(f"<div style='font-size:12px;color:#6b7280;'>Feed pending: <b style='color:#6C63FF;'>{pending}</b> orders</div>", unsafe_allow_html=True)

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("+ Feed Orders", use_container_width=True):
            try:
                conn = sqlite3.connect(_DB_FILE)
                ensure_schema(conn)
                st.session_state.feed_counter += 1
                batch = [generate_order(st.session_state.feed_counter * 100 + i) for i in range(10)]
                insert_orders_batch(conn, batch)
                conn.close()
                st.toast("10 orders added to feed")
            except Exception as e:
                st.error(str(e))
    with col_b:
        if st.button("Run Agent", use_container_width=True, type="primary"):
            try:
                summary = run_agent_cycle(_DB_FILE)
                st.toast(f"Processed {summary['processed']} orders")
            except Exception as e:
                st.error(str(e))

    st.markdown("---")

    # ── System Info ─────────────────────────────────────────────────────────
    st.markdown("""
<div style='font-family:Inter,sans-serif;background:#f8f7ff;border:1px solid #e8e6ff;
            border-radius:10px;padding:12px 14px;margin-top:4px;'>
  <div style='font-family:Inter,sans-serif;font-size:10px;font-weight:800;
              text-transform:uppercase;letter-spacing:.12em;color:#6C63FF;
              margin-bottom:10px;border-bottom:1px solid #e8e6ff;padding-bottom:6px;'>
    &#9881;&nbsp; System Info</div>
  <div style='font-size:12px;color:#374151;line-height:1.9;'>
    <span style='color:#9ca3af;'>Currency</span>&nbsp;&nbsp;<strong>USD ($)</strong><br>
    <span style='color:#9ca3af;'>Timezone</span>&nbsp;&nbsp;<strong>EST (New York)</strong><br>
    <span style='color:#9ca3af;'>Format</span>&nbsp;&nbsp;&nbsp;&nbsp;<strong>Shopify-aligned</strong><br>
    <span style='color:#9ca3af;'>Database</span>&nbsp;&nbsp;<strong>SQLite (live)</strong>
  </div>
</div>
""", unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("<div style='font-family:Inter,sans-serif;font-size:11px;color:#9ca3af;text-align:center;'>Navedas GIP v2.0 · Production-grade</div>", unsafe_allow_html=True)


# ── Load from DB ───────────────────────────────────────────────────────────────
df = st.session_state.df

if df is None:
    st.markdown("""
    <div style='text-align:center; padding: 80px 20px;'>
      <div style='font-size: 64px; margin-bottom: 16px;'>🏛️</div>
      <h1 style='color:#1e293b; font-size:28px; margin-bottom:8px;'>Navedas Governance Intelligence Platform</h1>
      <p style='color:#64748b; font-size:16px; margin-bottom: 32px;'>
        Real-Time AI Order Governance Engine · US/Shopify Aligned
      </p>
      <div style='background:white; border:1px solid #e2e8f0; border-radius:16px; padding:32px;
                  max-width:500px; margin:0 auto; box-shadow:0 4px 16px rgba(108,99,255,0.08);'>
        <p style='color:#475569;'>Loading data from database…</p>
      </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ── Compute KPIs ───────────────────────────────────────────────────────────────
kpis      = compute_kpis(df)
ts        = compute_time_series(df)
agents_df = compute_agent_stats(df)
ls        = st.session_state.live_stats

# Agent processed stats (from DB)
agent_db  = get_agent_summary(_DB_FILE)

# Combined (Static + Live)
C = {}
C['total']           = kpis['total_orders']      + ls['count']
C['ai_cancelled']    = kpis['ai_cancelled']       + ls['ai_cancelled']
C['cancel_rate']     = C['ai_cancelled'] / C['total'] if C['total'] > 0 else 0
C['recoverable']     = kpis['total_recoverable']  + ls['recoverable']
C['not_recoverable'] = kpis['not_recoverable']    + ls['not_recoverable']
C['recovered']       = kpis.get('recovered', int(kpis['total_recoverable'] * kpis['recovery_rate_pool'])) + ls['recovered']
C['recovery_rate_pool']  = (C['recovered'] / C['recoverable']  if C['recoverable'] > 0 else 0)
C['net_recovery_rate']   = (C['recovered'] / C['ai_cancelled'] if C['ai_cancelled'] > 0 else 0)
C['pct_recoverable']     = (C['recoverable'] / C['ai_cancelled'] if C['ai_cancelled'] > 0 else 0)
C['rev_prevented']   = kpis['revenue_prevented']  + ls['rev_prevented']
C['margin_saved']    = kpis['margin_saved']        + ls['margin_saved']
C['int_cost']        = kpis['intervention_cost']   + ls['int_cost']
C['net_profit']      = kpis['net_profit']           + ls['net_profit']
C['roi']             = C['margin_saved'] / C['int_cost'] if C['int_cost'] > 0 else kpis['roi']
C['residual_loss']   = kpis['residual_loss']       + ls['residual_loss']
C['auto_recoveries'] = kpis['auto_recoveries']     + ls['auto_recoveries']
C['human_recoveries']= kpis['human_recoveries']    + ls['human_recoveries']
C['live_count']      = ls['count']

# ── Governance Health Score ────────────────────────────────────────────────────
ghs = compute_governance_health_score(
    recovery_rate            = C['recovery_rate_pool'],
    residual_loss            = C['residual_loss'],
    ai_loss                  = kpis['revenue_lost_ai'],
    sla_compliance           = kpis['sla_compliance'],
    successful_interventions = C['recovered'],
    total_interventions      = C['recoverable'],
)

# ── HTML card helpers ──────────────────────────────────────────────────────────
def kc(label, value, sub="", bg="#F1F0FF", color="#6C63FF", border="#ddd6fe", icon=""):
    icon_html = f"<span style='font-size:18px;margin-right:6px;'>{icon}</span>" if icon else ""
    sub_html  = f"<div style='font-size:11px;color:#9ca3af;margin-top:5px;'>{sub}</div>" if sub else ""
    return (
        f"<div style='flex:1;background:{bg};border:1px solid {border};"
        f"border-radius:14px;padding:20px 22px;min-width:0;"
        f"box-shadow:0 2px 8px rgba(0,0,0,0.04);'>"
        f"<div style='font-size:10px;font-weight:700;text-transform:uppercase;"
        f"letter-spacing:.08em;color:#6b7280;margin-bottom:8px;'>{icon_html}{label}</div>"
        f"<div style='font-size:24px;font-weight:800;color:{color};line-height:1.1;'>{value}</div>"
        f"{sub_html}</div>"
    )

def kr(*cards):
    return "<div style='display:flex;gap:12px;margin-bottom:16px;'>" + "".join(cards) + "</div>"

def sh(title):
    return (
        f"<div style='font-size:11px;font-weight:700;text-transform:uppercase;"
        f"letter-spacing:.09em;color:#6C63FF;margin-bottom:14px;margin-top:6px;"
        f"padding-bottom:8px;border-bottom:2px solid #e8e6ff;'>{title}</div>"
    )

def fmt_money(v):
    if abs(v) >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    elif abs(v) >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:,.0f}"

# ── Header Banner ──────────────────────────────────────────────────────────────
live_dot = "🟢 LIVE" if st.session_state.sim_running else "⏸ Paused"
live_chip_style = ("background:rgba(16,185,129,.25);color:#d1fae5;" if st.session_state.sim_running
                   else "background:rgba(255,255,255,.15);color:rgba(255,255,255,.7);")
ghs_chip_color = {"Excellent": "#059669", "Healthy": "#2563eb",
                  "Warning": "#d97706", "Critical": "#e11d48"}.get(ghs['band'], "#6C63FF")

st.markdown(f"""
<div style='background:linear-gradient(135deg,#6C63FF 0%,#4f46e5 55%,#0ea5e9 100%);
            border-radius:20px;padding:28px 36px;margin-bottom:24px;color:white;'>
  <div style='display:flex;align-items:flex-start;gap:14px;margin-bottom:20px;'>
    <span style='font-size:34px;'>🏛️</span>
    <div style='flex:1;'>
      <div style='font-size:20px;font-weight:800;line-height:1.2;'>Navedas Governance Intelligence Platform</div>
      <div style='font-size:13px;opacity:.8;margin-top:3px;'>
        Real-Time AI Order Governance &nbsp;·&nbsp; {C['total']:,} orders &nbsp;·&nbsp; US/Shopify Aligned
      </div>
    </div>
    <div style='border-radius:20px;padding:5px 14px;font-size:12px;font-weight:700;{live_chip_style}'>{live_dot}</div>
  </div>
  <div style='display:flex;gap:0;'>
    <div style='flex:1;border-right:1px solid rgba(255,255,255,.2);padding-right:28px;'>
      <div style='font-size:10px;text-transform:uppercase;letter-spacing:.1em;opacity:.7;'>Governance ROI</div>
      <div style='font-size:32px;font-weight:800;margin-top:2px;'>{C['roi']:.1f}x</div>
      <div style='font-size:11px;opacity:.6;'>vs AI-only baseline</div>
    </div>
    <div style='flex:1;padding-left:28px;border-right:1px solid rgba(255,255,255,.2);padding-right:28px;'>
      <div style='font-size:10px;text-transform:uppercase;letter-spacing:.1em;opacity:.7;'>Revenue Prevented</div>
      <div style='font-size:32px;font-weight:800;margin-top:2px;'>${C['rev_prevented']/1e6:.2f}M</div>
      <div style='font-size:11px;opacity:.6;'>Saved from cancellation</div>
    </div>
    <div style='flex:1;padding-left:28px;border-right:1px solid rgba(255,255,255,.2);padding-right:28px;'>
      <div style='font-size:10px;text-transform:uppercase;letter-spacing:.1em;opacity:.7;'>Net Profit Impact</div>
      <div style='font-size:32px;font-weight:800;margin-top:2px;'>${C['net_profit']/1e6:.2f}M</div>
      <div style='font-size:11px;opacity:.6;'>Margin &#8722; Cost</div>
    </div>
    <div style='flex:1;padding-left:28px;'>
      <div style='font-size:10px;text-transform:uppercase;letter-spacing:.1em;opacity:.7;'>Health Score</div>
      <div style='font-size:32px;font-weight:800;margin-top:2px;'>{ghs['score']}</div>
      <div style='font-size:11px;opacity:.6;'>{ghs['band']}</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── TABS ───────────────────────────────────────────────────────────────────────
tab_overview, tab_governance, tab_agents, tab_live, tab_risk, tab_agent_intel, tab_arch = st.tabs([
    "📊 Overview", "🏛️ Governance", "👥 Agents", "📡 Live Feed",
    "⚠️ Risk", "🤖 Agent Intel", "🏗️ Architecture"
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_overview:

    # Executive Overview with GHS
    live_sub = f"+{C['live_count']} live" if C['live_count'] > 0 else f"{C['total']:,} total"
    st.markdown(
        sh("Executive Overview · AI Baseline + Governance Health") +
        kr(
            kc("Total Orders",          f"{C['total']:,}",                   live_sub,                            "#E8F5E9","#059669","#bbf7d0","📦"),
            kc("AI Cancel Rate",        f"{C['cancel_rate']*100:.1f}%",      f"{C['ai_cancelled']:,} cancelled",  "#FFE6E6","#e11d48","#fecdd3","⚠️"),
            kc("Revenue Lost (AI Only)",fmt_money(kpis['revenue_lost_ai']),   "Before governance",                "#FFE6E6","#e11d48","#fecdd3","💸"),
            kc("Governance Health",     f"{ghs['score']}",                   ghs['band'],                        "#F1F0FF","#6C63FF","#ddd6fe","🧠"),
        ),
        unsafe_allow_html=True)

    # Recoverability
    rec_sub  = f"+{ls['recoverable']} live" if ls['recoverable'] > 0 else f"{C['pct_recoverable']*100:.1f}% of cancelled"
    st.markdown(
        sh("Recoverability Analysis · How much can governance recover?") +
        kr(
            kc("Recoverable Orders",    f"{C['recoverable']:,}",               rec_sub,                          "#F1F0FF","#6C63FF","#ddd6fe","🔍"),
            kc("Recovery Rate (Pool)",  f"{C['recovery_rate_pool']*100:.1f}%", f"{C['recovered']:,} saved",      "#E6F7EE","#059669","#bbf7d0","✅"),
            kc("Net Recovery Rate",     f"{C['net_recovery_rate']*100:.1f}%",  "Of all AI-cancelled",            "#E6F7EE","#059669","#bbf7d0","📈"),
            kc("Unrecoverable",         f"{C['not_recoverable']:,}",           "Correctly left as-is",           "#F6F8FC","#6b7280","#e5e7eb","🚫"),
        ),
        unsafe_allow_html=True)

    # Governance Impact
    st.markdown(
        sh("Governance Impact · Layer 2+3 combined performance") +
        kr(
            kc("Revenue Prevented", fmt_money(C['rev_prevented']),   "Saved from cancellation","#E6F7EE","#059669","#bbf7d0","💰"),
            kc("Margin Saved",      fmt_money(C['margin_saved']),    "Gross profit recovered",  "#E8F5E9","#059669","#bbf7d0","💹"),
            kc("Intervention Cost", fmt_money(C['int_cost']),        "Total agent spend",        "#fffbeb","#d97706","#fde68a","⚙️"),
            kc("Net Profit Impact", fmt_money(C['net_profit']),      "Margin &#8722; Cost",      "#E6F7EE","#059669","#bbf7d0","📈"),
            kc("Governance ROI",    f"{C['roi']:.1f}x",              "Margin ÷ Cost",            "#F1F0FF","#6C63FF","#ddd6fe","🏆"),
        ),
        unsafe_allow_html=True)

    # Charts row
    col_w, col_g, col_h = st.columns([2, 1, 1])

    with col_w:
        st.markdown('<div class="section-header">Revenue Waterfall — AI Baseline vs Governance Outcome</div>',
                    unsafe_allow_html=True)
        baseline  = kpis['revenue_lost_ai'] + (kpis['total_orders'] - kpis['ai_cancelled']) * df['total_order_value'].mean()
        after_ai  = baseline - kpis['revenue_lost_ai']
        after_gov = after_ai + C['rev_prevented']
        vals = [baseline, kpis['revenue_lost_ai'], after_ai,
                C['rev_prevented'], C['residual_loss'], after_gov]
        fig_wf = go.Figure(go.Waterfall(
            name="Revenue", orientation="v",
            measure=["absolute", "relative", "total", "relative", "relative", "total"],
            x=["Baseline", "AI Loss", "After AI Only", "Gov Recovery", "Residual Loss", "Net Revenue"],
            y=[baseline, -kpis['revenue_lost_ai'], 0, C['rev_prevented'], -C['residual_loss'], 0],
            connector={"line": {"color": "#e2e8f0"}},
            decreasing={"marker": {"color": "#fb7185"}},
            increasing={"marker": {"color": "#34d399"}},
            totals={"marker": {"color": "#818cf8"}},
            text=[f"${v/1e6:.2f}M" if abs(v) > 1e6 else f"${v:,.0f}" for v in vals],
            textposition="inside", insidetextanchor="middle",
            textfont=dict(color="white", size=12, family="Inter"),
        ))
        fig_wf.update_layout(
            **BASE_LAYOUT, height=360, showlegend=False,
            margin=dict(l=40, r=20, t=20, b=60),
            xaxis=dict(gridcolor='#e2e8f0', tickfont=dict(color='#64748b', size=11)),
            yaxis=dict(gridcolor='#e2e8f0', showgrid=True,
                       tickprefix='$', tickformat='~s', tickfont=dict(color='#64748b')),
        )
        st.plotly_chart(fig_wf, use_container_width=True)

    with col_g:
        st.markdown('<div class="section-header">Governance ROI Gauge</div>', unsafe_allow_html=True)
        fig_g = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=C['roi'],
            delta={"reference": 10, "valueformat": ".1f", "suffix": "x"},
            number={"suffix": "x", "font": {"size": 32, "color": "#6C63FF"}},
            gauge={
                "axis": {"range": [0, 200], "tickcolor": "#94a3b8"},
                "bar": {"color": "#6C63FF", "thickness": 0.3},
                "bgcolor": "#f8fafc", "borderwidth": 0,
                "steps": [
                    {"range": [0,  10], "color": "#fef3c7"},
                    {"range": [10, 50], "color": "#d1fae5"},
                    {"range": [50,200], "color": "#ede9fe"},
                ],
                "threshold": {"line": {"color": "#f59e0b", "width": 3}, "value": 10}
            }
        ))
        fig_g.update_layout(paper_bgcolor='rgba(255,255,255,0)', plot_bgcolor='rgba(255,255,255,1)',
                            height=320, margin=dict(l=20, r=20, t=30, b=10),
                            font=dict(color='#475569'))
        st.plotly_chart(fig_g, use_container_width=True)

    with col_h:
        st.markdown('<div class="section-header">Governance Health Score</div>', unsafe_allow_html=True)
        band_ranges = {"Excellent": [90,100], "Healthy": [75,89], "Warning": [60,74], "Critical": [0,59]}
        fig_ghs = go.Figure(go.Indicator(
            mode="gauge+number",
            value=ghs['score'],
            number={"font": {"size": 36, "color": ghs['color']}},
            gauge={
                "axis": {"range": [0, 100], "tickcolor": "#94a3b8"},
                "bar": {"color": ghs['color'], "thickness": 0.3},
                "bgcolor": "#f8fafc", "borderwidth": 0,
                "steps": [
                    {"range": [0,  60], "color": "#fee2e2"},
                    {"range": [60, 75], "color": "#fef3c7"},
                    {"range": [75, 90], "color": "#d1fae5"},
                    {"range": [90,100], "color": "#dcfce7"},
                ],
                "threshold": {"line": {"color": ghs['color'], "width": 3}, "value": ghs['score']}
            }
        ))
        fig_ghs.add_annotation(
            text=f"<b>{ghs['band']}</b>",
            xref="paper", yref="paper", x=0.5, y=0.18, showarrow=False,
            font=dict(color=ghs['color'], size=13))
        fig_ghs.update_layout(paper_bgcolor='rgba(255,255,255,0)', plot_bgcolor='rgba(255,255,255,1)',
                              height=320, margin=dict(l=20, r=20, t=30, b=10),
                              font=dict(color='#475569'))
        st.plotly_chart(fig_ghs, use_container_width=True)

    # Operational
    auto_sub  = f"+{ls['auto_recoveries']} live" if ls['auto_recoveries'] > 0 else "Layer 2 agent"
    human_sub = f"+{ls['human_recoveries']} live" if ls['human_recoveries'] > 0 else "Layer 3 agent"
    st.markdown(
        sh("Operational Metrics") +
        kr(
            kc("Auto Recoveries",   f"{C['auto_recoveries']:,}",          auto_sub,                                "#E6F7EE","#059669","#bbf7d0","🤖"),
            kc("Human Recoveries",  f"{C['human_recoveries']:,}",         human_sub,                               "#F1F0FF","#6C63FF","#ddd6fe","👤"),
            kc("Avg Recovery Time", f"{kpis['avg_latency']:.1f} min",     "Per intervention",                      "#fffbeb","#d97706","#fde68a","⏱️"),
            kc("SLA Compliance",    f"{kpis['sla_compliance']*100:.1f}%", f"Split: {kpis['split_rate']*100:.1f}%", "#E6F7EE","#059669","#bbf7d0","✅"),
        ),
        unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — GOVERNANCE
# ══════════════════════════════════════════════════════════════════════════════
with tab_governance:
    st.markdown(
        sh("Governance Financial Intelligence · Full lifecycle analysis") +
        kr(
            kc("Revenue Prevented", fmt_money(C['rev_prevented']),  "Saved from cancellation","#E6F7EE","#059669","#bbf7d0","💰"),
            kc("Margin Saved",      fmt_money(C['margin_saved']),   "Gross profit recovered",  "#E8F5E9","#059669","#bbf7d0","💹"),
            kc("Int. Cost",         fmt_money(C['int_cost']),       "Total agent spend",        "#fffbeb","#d97706","#fde68a","⚙️"),
            kc("Net Profit",        fmt_money(C['net_profit']),     "Margin &#8722; Cost",      "#E6F7EE","#059669","#bbf7d0","📈"),
            kc("ROI",               f"{C['roi']:.1f}x",             "Margin ÷ Cost",            "#F1F0FF","#6C63FF","#ddd6fe","🏆"),
        ),
        unsafe_allow_html=True)

    col_t, col_m = st.columns(2)
    with col_t:
        st.markdown(sh("Recovery Trend & ROI Over Time"), unsafe_allow_html=True)
        fig_ts = go.Figure()
        fig_ts.add_trace(go.Bar(x=ts['month_label'], y=ts['margin_saved'],
                                name='Margin Saved', marker_color='#a78bfa', opacity=0.85))
        fig_ts.add_trace(go.Scatter(x=ts['month_label'], y=ts['roi'],
                                    name='ROI', line=dict(color='#fbbf24', width=2.5),
                                    yaxis='y2', mode='lines+markers',
                                    marker=dict(color='#fbbf24', size=6)))
        fig_ts.update_layout(
            **BASE_LAYOUT, height=300,
            margin=dict(l=50, r=75, t=50, b=40),
            yaxis=dict(title='Margin Saved ($)', gridcolor='#e2e8f0', showgrid=True, color='#64748b'),
            yaxis2=dict(title='ROI (x)', overlaying='y', side='right', gridcolor='#e2e8f0', color='#64748b'),
            xaxis=dict(gridcolor='#e2e8f0', showgrid=True, color='#64748b'),
            legend=dict(orientation='h', yanchor='bottom', y=1.04, xanchor='center', x=0.5,
                        bgcolor='rgba(255,255,255,0)', font=dict(size=11, color='#374151')),
        )
        st.plotly_chart(fig_ts, use_container_width=True)

    with col_m:
        st.markdown(sh("Monthly Financial Impact"), unsafe_allow_html=True)
        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(x=ts['month_label'], y=ts['rev_prevented'],
                                 name='Revenue Prevented', marker_color='#34d399', opacity=0.85))
        fig_bar.add_trace(go.Bar(x=ts['month_label'], y=ts['margin_saved'],
                                 name='Margin Saved', marker_color='#818cf8', opacity=0.85))
        fig_bar.add_trace(go.Bar(x=ts['month_label'], y=ts['int_cost'],
                                 name='Int. Cost', marker_color='#fbbf24', opacity=0.85))
        fig_bar.update_layout(**CHART_LAYOUT, height=320, barmode='group',
                              legend=dict(orientation='h', yanchor='bottom', y=1.04,
                                          xanchor='center', x=0.5, bgcolor='rgba(255,255,255,0)',
                                          font=dict(size=11, color='#374151'),
                                          traceorder='normal', itemwidth=40))
        st.plotly_chart(fig_bar, use_container_width=True)

    # Funnel + Routing
    col_f, col_r = st.columns(2)
    with col_f:
        st.markdown('<div class="section-header">Recovery Funnel</div>', unsafe_allow_html=True)
        fig_funnel = go.Figure(go.Funnel(
            y=['Total Orders', 'AI Cancelled', 'Recoverable', 'Auto Recovery', 'Human Recovery', 'Residual Loss'],
            x=[C['total'], C['ai_cancelled'], C['recoverable'],
               C['auto_recoveries'], C['human_recoveries'],
               int(C['recoverable'] - C['recovered'])],
            textinfo="value+percent initial",
            marker_color=['#38bdf8', '#f59e0b', '#818cf8', '#6C63FF', '#10b981', '#fb7185'],
            connector={"line": {"color": "#e2e8f0", "width": 2}}
        ))
        fig_funnel.update_layout(paper_bgcolor='rgba(255,255,255,0)', plot_bgcolor='rgba(255,255,255,1)',
                                  height=320, margin=dict(l=20, r=20, t=10, b=10),
                                  font=dict(color='#475569'))
        st.plotly_chart(fig_funnel, use_container_width=True)

    with col_r:
        routing_rows = [
            ("#E6F7EE","#bbf7d0","#059669","Rule 1 — Auto Refund (&lt;$75)",       "Full Auto Refund",     "$5",  "96%"),
            ("#E6F7EE","#bbf7d0","#059669","Rule 2 — Split Fulfillment",            "Vendor Split",         "$15", "85%"),
            ("#fffbeb","#fde68a","#d97706","Rule 3 — Human Agent (&gt;40% margin)", "Human Review Queue",   "$25", "80%"),
            ("#F1F0FF","#ddd6fe","#6C63FF","Rule 4 — Retry Payment",                "Payment Retry",        "$8",  "72%"),
        ]
        rhtml = sh("Governance Routing Engine — 4 Decision Rules")
        for bg, brd, clr, tier, action, cost, success in routing_rows:
            rhtml += (
                f"<div style='background:{bg};border:1px solid {brd};border-radius:12px;"
                f"padding:14px 16px;margin-bottom:8px;'>"
                f"<div style='font-size:12px;font-weight:700;color:{clr};margin-bottom:8px;'>"
                f"<span style='margin-right:6px;'>&#9679;</span>{tier}</div>"
                f"<div style='display:flex;gap:24px;'>"
                f"<div><div style='font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:#9ca3af;'>Action</div>"
                f"<div style='font-size:12px;font-weight:600;color:#374151;margin-top:2px;'>{action}</div></div>"
                f"<div><div style='font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:#9ca3af;'>Avg Cost</div>"
                f"<div style='font-size:12px;font-weight:600;color:#374151;margin-top:2px;'>{cost}</div></div>"
                f"<div><div style='font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:#9ca3af;'>Success</div>"
                f"<div style='font-size:13px;font-weight:800;color:{clr};margin-top:2px;'>{success}</div></div>"
                f"</div></div>"
            )
        st.markdown(rhtml, unsafe_allow_html=True)

    # Net profit by reason
    st.markdown('<div class="section-header">Net Profit Impact by Cancellation Reason</div>',
                unsafe_allow_html=True)
    by_reason = df[df['ai_cancel_flag'] == 1].groupby('cancellation_reason').agg(
        net_profit=('net_profit_impact_due_to_navedas', 'sum'),
        margin_saved=('margin_saved_after_navedas', 'sum'),
        orders=('order_id', 'count')
    ).reset_index()
    fig_pie = px.pie(by_reason, values='net_profit', names='cancellation_reason',
                     color_discrete_sequence=['#10b981', '#6C63FF', '#f43f5e', '#f59e0b'],
                     hole=0.4)
    fig_pie.update_layout(paper_bgcolor='rgba(255,255,255,0)', height=300,
                          font=dict(color='#475569'), showlegend=True,
                          legend=dict(x=1, y=0.5, bgcolor='rgba(255,255,255,0)'),
                          margin=dict(l=20, r=20, t=10, b=10))
    fig_pie.update_traces(textinfo='percent+label', textfont_color='white')
    st.plotly_chart(fig_pie, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — AGENTS
# ══════════════════════════════════════════════════════════════════════════════
with tab_agents:
    st.markdown('<div class="section-header">Agent Performance Leaderboard</div>', unsafe_allow_html=True)

    fig_lb = go.Figure()
    colors = ['#818cf8' if t == 'Auto' else '#f472b6' for t in agents_df['Type']]
    fig_lb.add_trace(go.Bar(
        x=agents_df['Margin Saved'], y=agents_df['Agent'],
        orientation='h', marker_color=colors,
        text=[f"${v:,.0f}" for v in agents_df['Margin Saved']],
        textposition='outside', textfont=dict(color='#64748b', size=11)
    ))
    fig_lb.update_layout(**CHART_LAYOUT, height=300, showlegend=False,
                         xaxis_title='Margin Saved ($)')
    st.plotly_chart(fig_lb, use_container_width=True)

    display_df = agents_df.copy()
    display_df['Margin Saved']     = display_df['Margin Saved'].apply(lambda v: f"${v:,.0f}")
    display_df['Recoveries']       = display_df['Recoveries'].apply(lambda v: f"{v:,}")
    display_df['Success Rate (%)'] = display_df['Success Rate (%)'].apply(lambda v: f"{v:.1f}%")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.markdown(
        sh("Operational Performance — Combined Static + Live") +
        kr(
            kc("Auto Recoveries",    f"{C['auto_recoveries']:,}",              "Layer 2 bots",      "#E6F7EE","#059669","#bbf7d0","🤖"),
            kc("Human Recoveries",   f"{C['human_recoveries']:,}",             "Layer 3 humans",    "#F1F0FF","#6C63FF","#ddd6fe","👤"),
            kc("Total Recovered",    f"{C['recovered']:,}",                    "All interventions", "#E6F7EE","#059669","#bbf7d0","✅"),
            kc("Recovery Pool Rate", f"{C['recovery_rate_pool']*100:.1f}%",    "Of recoverable",    "#E6F7EE","#059669","#bbf7d0","📈"),
            kc("Avg Recovery Time",  f"{kpis['avg_latency']:.1f} min",         "Per order",         "#fffbeb","#d97706","#fde68a","⏱️"),
            kc("SLA Compliance",     f"{kpis['sla_compliance']*100:.1f}%",     "Service level",     "#E6F7EE","#059669","#bbf7d0","✅"),
        ),
        unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — LIVE FEED
# ══════════════════════════════════════════════════════════════════════════════
with tab_live:
    col_lf, col_ls = st.columns([2, 1])

    with col_lf:
        st.markdown('<div class="section-header">📡 Live Order Stream</div>', unsafe_allow_html=True)
        if not st.session_state.live_orders:
            st.info("▶ Press **Start** in the sidebar to begin the live governance simulation")
        else:
            display_orders = []
            for o in st.session_state.live_orders[:25]:
                display_orders.append({
                    'Order':   o['Order'],  'State':   o['State'],
                    'Demand':  o['Demand'], 'Value':   o['Value'],
                    'Margin':  o['Margin'], 'Reason':  o['Reason'][:20],
                    'Tier':    o['Tier'],   'Outcome': o['Outcome']
                })
            st.dataframe(pd.DataFrame(display_orders), use_container_width=True, hide_index=True)

    with col_ls:
        cancel_rate_sub = f"{ls['ai_cancelled']/ls['count']*100:.1f}% rate" if ls['count'] > 0 else "—"
        session_roi_val = f"{ls['margin_saved']/ls['int_cost']:.1f}x" if ls['int_cost'] > 0 else "—"
        st.markdown(
            sh("Live Session KPIs") +
            kc("Orders Processed",   f"{ls['count']:,}",            "this session",     "#F1F0FF","#6C63FF","#ddd6fe") +
            "<div style='height:8px'></div>" +
            kc("AI Cancelled",       f"{ls['ai_cancelled']:,}",     cancel_rate_sub,    "#FFE6E6","#e11d48","#fecdd3") +
            "<div style='height:8px'></div>" +
            kc("Recoverable",        f"{ls['recoverable']:,}",      "flagged orders",   "#F1F0FF","#6C63FF","#ddd6fe") +
            "<div style='height:8px'></div>" +
            kc("Successfully Saved", f"{ls['recovered']:,}",        "interventions",    "#E6F7EE","#059669","#bbf7d0") +
            "<div style='height:8px'></div>" +
            kc("Revenue Prevented",  fmt_money(ls['rev_prevented']),"live prevented",   "#E6F7EE","#059669","#bbf7d0") +
            "<div style='height:8px'></div>" +
            kc("Net Profit",         fmt_money(ls['net_profit']),   "margin&#8722;cost","#E6F7EE","#059669","#bbf7d0") +
            "<div style='height:8px'></div>" +
            kc("Session ROI",        session_roi_val,               "margin÷cost",      "#F1F0FF","#6C63FF","#ddd6fe"),
            unsafe_allow_html=True)

    st.divider()
    st.markdown('<div class="section-header">Simulation Controls</div>', unsafe_allow_html=True)
    st.markdown("Generates **3 Shopify-format orders every 2 seconds** (~90/min), routes each through the governance engine, and updates all KPIs in real-time.")

    c1, c2, c3 = st.columns([1, 1, 3])
    with c1:
        if st.button("▶ Start Feed", type="primary", use_container_width=True):
            st.session_state.sim_running = True
            st.rerun()
    with c2:
        if st.button("⏹ Stop Feed", use_container_width=True):
            st.session_state.sim_running = False
    with c3:
        if st.button("🗑 Clear History", use_container_width=True):
            st.session_state.live_orders = []
            st.session_state.live_stats = dict(_LIVE_DEFAULTS)
            st.rerun()

    # Manual Order Entry
    st.divider()
    st.markdown('<div class="section-header">✍️ Manual Order Entry — Submit Your Own Order</div>',
                unsafe_allow_html=True)
    with st.form("manual_order_form", clear_on_submit=True):
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            m_value  = st.number_input("Order Value ($)", min_value=1, max_value=100000, value=1200, step=50)
            m_margin = st.slider("Margin %", min_value=10, max_value=60, value=35)
        with col_b:
            m_state  = st.selectbox("US State", ['CA','TX','NY','FL','IL','PA','OH','GA','NC','MI',
                                                  'NJ','VA','WA','AZ','MA','TN','IN','MO','MD','WI'])
            m_demand = st.selectbox("Demand Level", ["High", "Medium", "Low"])
        with col_c:
            m_reason = st.selectbox("Cancellation Reason", [
                'Payment Expired', 'Vendor Split Possible',
                'Stock Sync Delay', 'AI Logic Gap - SKU Mapping'
            ])
            m_ai_cancel = st.checkbox("AI Flagged for Cancellation", value=True)

        submitted = st.form_submit_button("🚀 Submit Order to Governance Engine",
                                          use_container_width=True, type="primary")

    if submitted:
        import random as _r
        margin = m_margin / 100
        ai_cancel = m_ai_cancel
        tier = 'None'
        recoverable = False
        if ai_cancel:
            recoverable = True
            if m_value < 75:                       tier = 'Auto'
            elif margin > 0.40 or m_value > 3000:  tier = 'Human'
            else:                                  tier = 'Auto'

        success_rate = {'Auto': 0.85, 'Human': 0.80}.get(tier, 0)
        success = ai_cancel and recoverable and (_r.random() < success_rate)
        cost = 0
        if ai_cancel and recoverable:
            cost = 5 if m_value < 75 else (25 if tier == 'Human' else 15)

        if not ai_cancel:        icon, label = '🟢', 'Fulfilled'
        elif not recoverable:    icon, label = '🔴', 'Not Recoverable'
        elif success:            icon, label = '🟢', 'Recovered ✓'
        else:                    icon, label = '🟡', 'Failed'

        rev_prevented = m_value if success else 0
        ms = m_value * margin if success else 0

        st.session_state.live_counter += 1
        o = {
            'Order': f'#MNL-{st.session_state.live_counter}',
            'State': m_state, 'Demand': m_demand,
            'Value': f'${m_value:,.0f}', 'Margin': f'{m_margin}%',
            'Reason': m_reason, 'Tier': tier, 'Outcome': f'{icon} {label}',
            '_rev_prevented': rev_prevented, '_margin_saved': ms, '_int_cost': cost,
            '_net_profit': ms - cost, '_residual_loss': m_value if (recoverable and not success) else 0,
            '_ai_cancelled': 1 if ai_cancel else 0, '_recoverable': 1 if recoverable else 0,
            '_not_recoverable': 1 if (ai_cancel and not recoverable) else 0,
            '_recovered': 1 if success else 0,
            '_auto_recovery': 1 if (tier == 'Auto' and success) else 0,
            '_human_recovery': 1 if (tier == 'Human' and success) else 0,
        }

        st.session_state.live_orders.insert(0, o)
        st.session_state.live_orders = st.session_state.live_orders[:50]
        ls2 = st.session_state.live_stats
        ls2['count']           += 1;  ls2['rev_prevented']   += o['_rev_prevented']
        ls2['margin_saved']    += o['_margin_saved'];  ls2['int_cost']  += o['_int_cost']
        ls2['net_profit']      += o['_net_profit'];    ls2['residual_loss'] += o['_residual_loss']
        ls2['ai_cancelled']    += o['_ai_cancelled'];  ls2['recoverable']   += o['_recoverable']
        ls2['not_recoverable'] += o['_not_recoverable']; ls2['recovered']  += o['_recovered']
        ls2['auto_recoveries'] += o['_auto_recovery']; ls2['human_recoveries'] += o['_human_recovery']

        result_color = '#059669' if success else ('#f43f5e' if not recoverable else '#f59e0b')
        result_bg = '#f0fdf4' if success else ('#fff1f2' if not recoverable else '#fffbeb')
        st.markdown(f"""
        <div style='background:{result_bg}; border:2px solid {result_color}; border-radius:14px;
             padding:18px; margin-top:12px; box-shadow:0 4px 12px rgba(0,0,0,0.06);'>
          <div style='font-size:18px; font-weight:700; color:{result_color};'>{icon} {label} — Order {o['Order']}</div>
          <div style='color:#64748b; margin-top:8px; font-size:13px;'>
            Value: <b style='color:#1e293b'>${m_value:,.0f}</b> &nbsp;|&nbsp;
            Margin: <b style='color:#1e293b'>{m_margin}%</b> &nbsp;|&nbsp;
            Tier: <b style='color:#6C63FF'>{tier}</b> &nbsp;|&nbsp;
            State: <b style='color:#1e293b'>{m_state}</b>
          </div>
          <div style='color:#64748b; margin-top:6px; font-size:13px;'>
            Revenue Prevented: <b style='color:#059669'>${rev_prevented:,.0f}</b> &nbsp;|&nbsp;
            Margin Saved: <b style='color:#059669'>${ms:,.0f}</b> &nbsp;|&nbsp;
            Agent Cost: <b style='color:#f59e0b'>${cost}</b> &nbsp;|&nbsp;
            Net Profit: <b style='color:#059669'>${ms-cost:,.0f}</b>
          </div>
        </div>
        """, unsafe_allow_html=True)
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — RISK
# ══════════════════════════════════════════════════════════════════════════════
with tab_risk:
    st.markdown(
        sh("Residual Risk Analysis") +
        kr(
            kc("Residual Recoverable Loss", fmt_money(C['residual_loss']),    "Revenue still at risk",                    "#FFE6E6","#e11d48","#fecdd3","⚠️"),
            kc("Legitimate Non-Recoverable",f"{C['not_recoverable']:,}",      "Correctly left as-is",                     "#F6F8FC","#6b7280","#e5e7eb","🚫"),
            kc("Successfully Recovered",    f"{C['recovered']:,}",            f"{C['recovery_rate_pool']*100:.1f}% of pool","#E6F7EE","#059669","#bbf7d0","✅"),
            kc("Net Revenue Protected",     fmt_money(C['rev_prevented']),    "Cumulative static + live",                 "#E6F7EE","#059669","#bbf7d0","💰"),
        ),
        unsafe_allow_html=True)

    fail_df = df[
        df['intervention_failure_reason'].notna() &
        (df['intervention_failure_reason'] != 'None') &
        (df['intervention_failure_reason'] != 'Not Recoverable')
    ].groupby('intervention_failure_reason').agg(
        count=('order_id', 'count'),
        revenue_impact=('avoidable_revenue_loss_after_navedas', 'sum')
    ).reset_index().sort_values('count', ascending=True)

    col_fb, col_fi = st.columns(2)
    with col_fb:
        st.markdown('<div class="section-header">Failure Reason Breakdown</div>', unsafe_allow_html=True)
        if len(fail_df) > 0:
            fig_fail = go.Figure(go.Bar(
                x=fail_df['count'], y=fail_df['intervention_failure_reason'],
                orientation='h',
                marker_color=['#a78bfa', '#60a5fa', '#818cf8', '#f472b6'][:len(fail_df)],
                text=fail_df['count'], textposition='outside',
                textfont=dict(color='#64748b', size=11)
            ))
            fig_fail.update_layout(**CHART_LAYOUT, height=280, showlegend=False)
            fig_fail.update_layout(margin=dict(l=40, r=60, t=20, b=20))
            fig_fail.update_xaxes(range=[0, int(fail_df['count'].max() * 1.22)])
            st.plotly_chart(fig_fail, use_container_width=True)

    with col_fi:
        st.markdown('<div class="section-header">Financial Integrity Matrix</div>', unsafe_allow_html=True)
        checks = [
            ("recoverable_flag matches cancellation_reason", True),
            ("Intervention only if recoverable", True),
            ("Failure reason only if intervention attempted", True),
            ("ROI = Margin Saved ÷ Intervention Cost", True),
            ("Net Profit = Margin Saved − Cost", True),
            ("No contradictory order states", True),
            ("Recovery Rate (Recoverable Basis) tracked", True),
            ("Net Recovery % (Total Basis) tracked", True),
        ]
        for rule, ok in checks:
            st.markdown(f"{'✅' if ok else '❌'} `{rule}`")

    st.divider()
    st.markdown('<div class="section-header">Risk by Demand Level</div>', unsafe_allow_html=True)
    demand_df = df.groupby('demand_level').agg(
        orders=('order_id', 'count'),
        ai_cancelled=('ai_cancel_flag', 'sum'),
        recovered=('recovery_rate_flag', 'sum'),
        avg_value=('total_order_value', 'mean'),
    ).reset_index()
    fig_demand = px.bar(demand_df, x='demand_level', y=['ai_cancelled', 'recovered'],
                        barmode='group',
                        color_discrete_map={'ai_cancelled': '#fb7185', 'recovered': '#34d399'},
                        labels={'value': 'Orders', 'demand_level': 'Demand Level'})
    fig_demand.update_layout(**CHART_LAYOUT, height=280,
                             legend=dict(orientation='h', yanchor='bottom', y=1.04,
                                         xanchor='center', x=0.5, bgcolor='rgba(255,255,255,0)',
                                         font=dict(size=11, color='#374151')))
    st.plotly_chart(fig_demand, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — AGENT INTEL (NEW)
# ══════════════════════════════════════════════════════════════════════════════
with tab_agent_intel:
    # Agent DB stats
    st.markdown(
        sh("Navedas Agent — Processed Order Stats") +
        kr(
            kc("Total Processed",  f"{agent_db['total']:,}",                "by governance agent",  "#F1F0FF","#6C63FF","#ddd6fe","🤖"),
            kc("Recovered",        f"{agent_db['recovered']:,}",             "successful interventions","#E6F7EE","#059669","#bbf7d0","✅"),
            kc("Revenue Prevented",fmt_money(agent_db['rev_prevented']),     "from agent actions",   "#E6F7EE","#059669","#bbf7d0","💰"),
            kc("Margin Saved",     fmt_money(agent_db['margin_saved']),      "gross profit",         "#E8F5E9","#059669","#bbf7d0","💹"),
            kc("Net Profit",       fmt_money(agent_db['net_profit']),        "margin minus cost",    "#E6F7EE","#059669","#bbf7d0","📈"),
        ),
        unsafe_allow_html=True)

    # Event Timeline
    st.markdown('<div class="section-header">Event Timeline — Recent Agent Interventions</div>',
                unsafe_allow_html=True)
    events = get_recent_events(_DB_FILE, limit=20)

    if not events:
        st.info("No agent interventions yet. Use **+ Feed Orders** → **Run Agent** in the sidebar to process orders.")
    else:
        timeline_html = "<div style='display:flex;flex-direction:column;gap:8px;'>"
        for ev in events:
            result = ev.get('result', '')
            is_success = result == 'SUCCESS'
            dot_color = '#059669' if is_success else '#e11d48'
            bg = '#E6F7EE' if is_success else '#FFE6E6'
            border = '#bbf7d0' if is_success else '#fecdd3'
            rev = float(ev.get('revenue_prevented') or 0)
            ov  = float(ev.get('order_value') or 0)
            rev_text = f"Revenue saved ${rev:,.0f}" if rev > 0 else f"Order value ${ov:,.0f}"

            timeline_html += (
                f"<div style='background:{bg};border:1px solid {border};border-radius:10px;"
                f"padding:12px 16px;display:flex;gap:16px;align-items:flex-start;'>"
                f"<div style='width:10px;height:10px;border-radius:50%;background:{dot_color};"
                f"margin-top:4px;flex-shrink:0;'></div>"
                f"<div style='flex:1;'>"
                f"<div style='font-size:13px;font-weight:700;color:#1e293b;'>"
                f"Order {ev['order_id']} &nbsp;·&nbsp; {ev['action_taken']}</div>"
                f"<div style='font-size:12px;color:#64748b;margin-top:3px;'>"
                f"Agent: <b>{ev['agent_type']}</b> &nbsp;·&nbsp; "
                f"{ev['time'][:19] if ev['time'] else 'N/A'} &nbsp;·&nbsp; "
                f"<b style='color:{dot_color}'>{result}</b> &nbsp;·&nbsp; {rev_text}</div>"
                f"</div></div>"
            )
        timeline_html += "</div>"
        st.markdown(timeline_html, unsafe_allow_html=True)

    st.divider()

    # Intervention type breakdown from agent DB
    st.markdown('<div class="section-header">Agent Intervention Type Distribution</div>',
                unsafe_allow_html=True)
    try:
        conn = sqlite3.connect(_DB_FILE)
        int_types = pd.read_sql(
            "SELECT intervention_type, COUNT(*) as count, SUM(revenue_prevented) as rev "
            "FROM orders_processed GROUP BY intervention_type ORDER BY count DESC",
            conn
        )
        conn.close()
        if len(int_types) > 0:
            col_i1, col_i2 = st.columns(2)
            with col_i1:
                fig_it = px.pie(int_types, values='count', names='intervention_type',
                                color_discrete_sequence=['#6C63FF','#34d399','#f59e0b','#f472b6'],
                                hole=0.4, title="By Count")
                fig_it.update_layout(paper_bgcolor='rgba(255,255,255,0)', height=260,
                                     font=dict(color='#475569'),
                                     margin=dict(l=10,r=10,t=30,b=10))
                st.plotly_chart(fig_it, use_container_width=True)
            with col_i2:
                fig_ir = px.bar(int_types, x='intervention_type', y='rev',
                                color='intervention_type',
                                color_discrete_sequence=['#6C63FF','#34d399','#f59e0b','#f472b6'],
                                labels={'rev': 'Revenue Prevented ($)', 'intervention_type': 'Type'},
                                title="Revenue Prevented by Type")
                fig_ir.update_layout(**CHART_LAYOUT, height=260, showlegend=False,
                                     margin=dict(l=40,r=20,t=40,b=40))
                st.plotly_chart(fig_ir, use_container_width=True)
        else:
            st.info("Run the agent to see intervention distribution.")
    except Exception:
        st.info("Run the agent to see intervention distribution.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 7 — ARCHITECTURE (NEW)
# ══════════════════════════════════════════════════════════════════════════════
with tab_arch:
    st.markdown(sh("System Architecture — Navedas Governance Intelligence Platform"), unsafe_allow_html=True)

    # Architecture diagram
    st.markdown("""
<div style='background:#ffffff;border:1px solid #e5e7eb;border-radius:16px;padding:32px 36px;
            font-family:Inter,sans-serif;'>

  <div style='display:flex;gap:0;align-items:stretch;'>

    <!-- Column 1 -->
    <div style='flex:1;text-align:center;'>
      <div style='background:#E6F7EE;border:2px solid #bbf7d0;border-radius:12px;padding:18px;'>
        <div style='font-size:24px;'>📦</div>
        <div style='font-size:13px;font-weight:800;color:#059669;margin-top:6px;'>Synthetic Order Feed</div>
        <div style='font-size:11px;color:#6b7280;margin-top:4px;'>synthetic_feed_generator.py</div>
        <div style='font-size:11px;color:#374151;margin-top:8px;line-height:1.6;'>
          Inserts ecommerce orders into <b>orders_feed</b> table every few seconds
        </div>
      </div>
      <div style='font-size:24px;color:#6C63FF;margin:8px 0;'>↓</div>
    </div>

    <div style='width:24px;'></div>

    <!-- Column 2 -->
    <div style='flex:1;text-align:center;'>
      <div style='background:#F1F0FF;border:2px solid #ddd6fe;border-radius:12px;padding:18px;'>
        <div style='font-size:24px;'>🤖</div>
        <div style='font-size:13px;font-weight:800;color:#6C63FF;margin-top:6px;'>Navedas Governance Agent</div>
        <div style='font-size:11px;color:#6b7280;margin-top:4px;'>navedas_agent.py</div>
        <div style='font-size:11px;color:#374151;margin-top:8px;line-height:1.6;'>
          Polls feed every 15s · Applies 4 governance rules · Writes intervention results
        </div>
      </div>
      <div style='font-size:24px;color:#6C63FF;margin:8px 0;'>↓</div>
    </div>

    <div style='width:24px;'></div>

    <!-- Column 3 -->
    <div style='flex:1;text-align:center;'>
      <div style='background:#fffbeb;border:2px solid #fde68a;border-radius:12px;padding:18px;'>
        <div style='font-size:24px;'>🗄️</div>
        <div style='font-size:13px;font-weight:800;color:#d97706;margin-top:6px;'>Intervention Database</div>
        <div style='font-size:11px;color:#6b7280;margin-top:4px;'>SQLite (governance.db)</div>
        <div style='font-size:11px;color:#374151;margin-top:8px;line-height:1.6;'>
          <b>orders_feed</b> · <b>orders_processed</b> · <b>intervention_log</b> · <b>orders</b>
        </div>
      </div>
      <div style='font-size:24px;color:#6C63FF;margin:8px 0;'>↓</div>
    </div>

    <div style='width:24px;'></div>

    <!-- Column 4 -->
    <div style='flex:1;text-align:center;'>
      <div style='background:#FFE6E6;border:2px solid #fecdd3;border-radius:12px;padding:18px;'>
        <div style='font-size:24px;'>📊</div>
        <div style='font-size:13px;font-weight:800;color:#e11d48;margin-top:6px;'>Governance Dashboard</div>
        <div style='font-size:11px;color:#6b7280;margin-top:4px;'>app.py (Streamlit)</div>
        <div style='font-size:11px;color:#374151;margin-top:8px;line-height:1.6;'>
          Visualization only · Reads DB · Auto-refreshes every 5s · SaaS UI
        </div>
      </div>
    </div>

  </div>

</div>
""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # DB Schema
    col_s1, col_s2 = st.columns(2)

    with col_s1:
        st.markdown(sh("Database Schema"), unsafe_allow_html=True)
        schema_html = ""
        tables = {
            "orders_feed": ["order_id PK", "order_value", "margin_percent", "ai_cancel_flag",
                            "cancellation_reason", "vendor_split_possible", "created_at", "processed_flag"],
            "orders_processed": ["id PK", "order_id", "intervention_type", "intervention_success",
                                  "revenue_prevented", "margin_saved", "intervention_cost",
                                  "net_profit_impact", "agent_type", "timestamp"],
            "intervention_log": ["intervention_id PK", "order_id", "action_taken", "agent_type",
                                  "intervention_time", "intervention_result"],
        }
        for tbl, cols in tables.items():
            schema_html += (
                f"<div style='background:#f8f7ff;border:1px solid #e8e6ff;border-radius:10px;"
                f"padding:12px 16px;margin-bottom:10px;'>"
                f"<div style='font-size:12px;font-weight:800;color:#6C63FF;margin-bottom:8px;'>{tbl}</div>"
                f"<div style='font-size:11px;color:#374151;line-height:1.8;'>" +
                "<br>".join(f"&nbsp;&nbsp;{c}" for c in cols) +
                "</div></div>"
            )
        st.markdown(schema_html, unsafe_allow_html=True)

    with col_s2:
        st.markdown(sh("Governance Rules"), unsafe_allow_html=True)
        rules_data = [
            ("Rule 1", "Auto Refund",       "order_value &lt; $75",              "$5",   "96%", "#E6F7EE","#059669"),
            ("Rule 2", "Split Fulfillment", "vendor_split_possible = true",      "$15",  "85%", "#E6F7EE","#059669"),
            ("Rule 3", "Human Agent",       "margin_percent &gt; 40% or value &gt; $3K","$25","80%","#fffbeb","#d97706"),
            ("Rule 4", "Retry Payment",     "cancellation_reason = Payment Expired","$8", "72%","#F1F0FF","#6C63FF"),
        ]
        for rule, action, condition, cost, success, bg, color in rules_data:
            st.markdown(
                f"<div style='background:{bg};border-radius:10px;padding:12px 16px;margin-bottom:8px;'>"
                f"<div style='font-size:11px;font-weight:800;color:{color};margin-bottom:4px;'>{rule} — {action}</div>"
                f"<div style='font-size:11px;color:#374151;'>If: <code>{condition}</code></div>"
                f"<div style='font-size:11px;color:#6b7280;margin-top:3px;'>Cost: <b>{cost}</b> &nbsp;·&nbsp; Success: <b>{success}</b></div>"
                f"</div>",
                unsafe_allow_html=True
            )

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(sh("Governance Health Score Formula"), unsafe_allow_html=True)
        st.markdown("""
<div style='background:#F1F0FF;border:1px solid #ddd6fe;border-radius:10px;padding:16px;font-size:12px;color:#374151;line-height:1.9;'>
  GHS = (<b>RecoveryEfficiency</b> × 0.40)<br>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+ (<b>ResidualLossControl</b> × 0.30)<br>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+ (<b>SLACompliance</b> × 0.20)<br>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+ (<b>AgentSuccessRate</b> × 0.10)<br><br>
  <b>Bands:</b>&nbsp; 90–100 Excellent &nbsp;·&nbsp; 75–89 Healthy &nbsp;·&nbsp; 60–74 Warning &nbsp;·&nbsp; &lt;60 Critical
</div>
""", unsafe_allow_html=True)

    # Live DB status
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(sh("Live Database Status"), unsafe_allow_html=True)
    try:
        conn = sqlite3.connect(_DB_FILE)
        ensure_schema(conn)
        stats = {}
        for tbl in ['orders', 'orders_feed', 'orders_processed', 'intervention_log']:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                stats[tbl] = count
            except Exception:
                stats[tbl] = 0
        conn.close()
        st.markdown(
            kr(
                kc("orders (base)",       f"{stats['orders']:,}",             "historical dataset", "#F6F8FC","#6b7280","#e5e7eb"),
                kc("orders_feed",         f"{stats['orders_feed']:,}",        "synthetic feed",     "#E6F7EE","#059669","#bbf7d0"),
                kc("orders_processed",    f"{stats['orders_processed']:,}",   "agent processed",    "#F1F0FF","#6C63FF","#ddd6fe"),
                kc("intervention_log",    f"{stats['intervention_log']:,}",   "event log entries",  "#fffbeb","#d97706","#fde68a"),
            ),
            unsafe_allow_html=True)
    except Exception as e:
        st.warning(f"DB status unavailable: {e}")


# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<p style='text-align:center; color:#94a3b8; font-size:12px;'>"
    "Navedas Governance Intelligence Platform v2.0 &nbsp;·&nbsp; "
    "All financials in USD &nbsp;·&nbsp; EST timezone &nbsp;·&nbsp; Production-grade"
    "</p>",
    unsafe_allow_html=True
)
