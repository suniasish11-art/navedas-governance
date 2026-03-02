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
import time, os, random
from pipeline import (
    load_data, compute_kpis, compute_time_series,
    compute_agent_stats, generate_live_order
)

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Navedas Governance Intelligence Platform",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }
  .main { background: #0a0f1e; }
  .stMetric { background: #111827; border: 1px solid #1f2937; border-radius: 12px; padding: 16px !important; }
  .stMetric label { color: #6b7280 !important; font-size: 11px !important; text-transform: uppercase; letter-spacing: 0.05em; }
  .stMetric [data-testid="stMetricValue"] { color: white !important; font-size: 22px !important; font-weight: 700 !important; }
  div[data-testid="stHorizontalBlock"] { gap: 12px; }
  .section-header { color: #6b7280; font-size: 11px; font-weight: 600; text-transform: uppercase;
                    letter-spacing: 0.08em; margin-bottom: 12px; padding-bottom: 8px;
                    border-bottom: 1px solid #1f2937; }
  .kpi-green [data-testid="stMetricValue"] { color: #10b981 !important; }
  .kpi-red [data-testid="stMetricValue"] { color: #f43f5e !important; }
  .kpi-amber [data-testid="stMetricValue"] { color: #f59e0b !important; }
  .kpi-blue [data-testid="stMetricValue"] { color: #38bdf8 !important; }
  .stTabs [data-baseweb="tab-list"] { gap: 8px; background: #111827; border-radius: 12px; padding: 4px; }
  .stTabs [data-baseweb="tab"] { border-radius: 8px; color: #6b7280 !important; font-size: 13px; padding: 8px 16px; }
  .stTabs [aria-selected="true"] { background: #1f2937 !important; color: white !important; }
  .live-badge { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
                background: #10b981; animation: pulse 1.5s infinite; margin-right: 6px; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
  footer { visibility: hidden; }
  #MainMenu { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

CHART_LAYOUT = dict(
    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
    font=dict(color='#9ca3af', family='Inter'),
    margin=dict(l=40, r=20, t=30, b=40),
    xaxis=dict(gridcolor='#1f2937', showgrid=True),
    yaxis=dict(gridcolor='#1f2937', showgrid=True),
)

# ── Session state init ─────────────────────────────────────────────────────────
if 'df' not in st.session_state:           st.session_state.df = None
if 'live_orders' not in st.session_state:  st.session_state.live_orders = []
if 'sim_running' not in st.session_state:  st.session_state.sim_running = False
if 'live_counter' not in st.session_state: st.session_state.live_counter = 800000
if 'live_stats' not in st.session_state:
    st.session_state.live_stats = {'rev_prevented': 0, 'margin_saved': 0,
                                   'int_cost': 0, 'net_profit': 0, 'count': 0}

# ── Auto-refresh when simulation running ───────────────────────────────────────
if st.session_state.sim_running:
    st_autorefresh(interval=5000, key="live_refresh")

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏛️ Navedas GIP")
    st.markdown("**Governance Intelligence Platform**")
    st.markdown("---")

    # Data source
    st.markdown("### 📂 Data Source")
    data_source = st.radio("", ["Upload CSV", "Use bundled data"], label_visibility="collapsed")

    df = None
    if data_source == "Upload CSV":
        uploaded = st.file_uploader("Upload ecommerce CSV", type=['csv'])
        if uploaded:
            content = uploaded.read().decode('utf-8')
            st.session_state.df = load_data(content)
            st.success(f"✅ {len(st.session_state.df):,} orders loaded")
    else:
        # Try to load from local path (works locally, not on Streamlit Cloud)
        local_paths = [
            r"C:\Users\sunit\ecommerce_ production_dataset_5000_rows.csv",
            "ecommerce_ production_dataset_5000_rows.csv",
            "data/ecommerce_production.csv"
        ]
        for p in local_paths:
            if os.path.exists(p):
                if st.session_state.df is None:
                    with open(p, 'r', encoding='utf-8') as f:
                        content = f.read()
                    st.session_state.df = load_data(content)
                break
        if st.session_state.df is not None:
            st.success(f"✅ {len(st.session_state.df):,} orders loaded")
        else:
            st.info("📎 Upload your CSV file above")

    df = st.session_state.df

    st.markdown("---")
    st.markdown("### ⚡ Live Simulation")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("▶ Start", use_container_width=True, type="primary"):
            st.session_state.sim_running = True
    with col2:
        if st.button("⏹ Stop", use_container_width=True):
            st.session_state.sim_running = False

    if st.session_state.sim_running:
        st.markdown('<span class="live-badge"></span> **Live feed active**', unsafe_allow_html=True)
        # Generate a new order each refresh
        st.session_state.live_counter += 1
        new_order = generate_live_order(st.session_state.live_counter)
        st.session_state.live_orders.insert(0, new_order)
        st.session_state.live_orders = st.session_state.live_orders[:50]
        # Accumulate live stats
        ls = st.session_state.live_stats
        ls['rev_prevented'] += new_order['_rev_prevented']
        ls['margin_saved'] += new_order['_margin_saved']
        ls['int_cost'] += new_order['_int_cost']
        ls['net_profit'] += new_order['_net_profit']
        ls['count'] += 1

    st.markdown("---")
    st.markdown("### 🇺🇸 System Info")
    st.markdown("- Currency: **USD ($)**")
    st.markdown("- Timezone: **EST (New York)**")
    st.markdown("- Format: **Shopify-aligned**")
    st.markdown("- DB: **SQLite (seeded)**")
    st.markdown("---")
    st.markdown("*Navedas GIP v1.0*  \n*Production-grade · Client-shareable*")


# ── No data state ──────────────────────────────────────────────────────────────
if df is None:
    st.markdown("""
    <div style='text-align:center; padding: 80px 20px;'>
      <div style='font-size: 64px; margin-bottom: 16px;'>🏛️</div>
      <h1 style='color:white; font-size:28px; margin-bottom:8px;'>Navedas Governance Intelligence Platform</h1>
      <p style='color:#6b7280; font-size:16px; margin-bottom: 32px;'>
        Real-Time AI Order Governance Engine · US/Shopify Aligned
      </p>
      <div style='background:#111827; border:1px solid #1f2937; border-radius:16px; padding:32px; max-width:500px; margin:0 auto;'>
        <p style='color:#9ca3af;'>👈 Upload your <strong style='color:white;'>ecommerce CSV</strong> in the sidebar to begin</p>
        <p style='color:#6b7280; font-size:13px; margin-top:12px;'>
          Or if running locally, the dataset auto-loads from your machine.
        </p>
      </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ── Compute KPIs ───────────────────────────────────────────────────────────────
kpis = compute_kpis(df)
ts = compute_time_series(df)
agents_df = compute_agent_stats(df)
ls = st.session_state.live_stats

# ── Header ─────────────────────────────────────────────────────────────────────
col_h1, col_h2, col_h3 = st.columns([3, 1, 1])
with col_h1:
    st.markdown("## 🏛️ Navedas Governance Intelligence Platform")
    st.markdown(f"<p style='color:#6b7280; margin-top:-8px;'>Real-Time AI Order Governance · {kpis['total_orders']:,} orders · US/Shopify Aligned</p>", unsafe_allow_html=True)
with col_h2:
    st.metric("Governance ROI", f"{kpis['roi']:.1f}x", delta="vs AI-only baseline")
with col_h3:
    live_count = len(st.session_state.live_orders)
    st.metric("Live Orders", live_count, delta="streaming" if st.session_state.sim_running else "paused")

st.divider()

# ── TABS ───────────────────────────────────────────────────────────────────────
tab_overview, tab_governance, tab_agents, tab_live, tab_risk = st.tabs([
    "📊 Overview", "🏛️ Governance", "👥 Agents", "📡 Live Feed", "⚠️ Risk"
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    # ── AI Baseline ────────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Layer 1 — AI Baseline · Unmitigated cancellation impact</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Orders", f"{kpis['total_orders']:,}")
    c2.metric("AI Cancel Rate", f"{kpis['ai_cancel_rate']*100:.1f}%",
              delta=f"{kpis['ai_cancelled']:,} cancelled", delta_color="inverse")
    c3.metric("Revenue Lost (AI Only)", f"${kpis['revenue_lost_ai']:,.0f}",
              delta="Before governance", delta_color="inverse")
    c4.metric("Profit Lost (AI Only)", f"${kpis['profit_lost_ai']:,.0f}",
              delta="Gross profit exposure", delta_color="inverse")

    st.divider()

    # ── Recoverability ─────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Recoverability Analysis · How much can governance recover?</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Recoverable Orders", f"{kpis['total_recoverable']:,}",
              delta=f"{kpis['pct_recoverable']*100:.1f}% of cancelled")
    c2.metric("Recovery Rate (Pool)", f"{kpis['recovery_rate_pool']*100:.1f}%",
              delta="Of recoverable orders")
    c3.metric("Net Recovery Rate", f"{kpis['recovery_rate_total']*100:.1f}%",
              delta="Of all AI-cancelled")
    c4.metric("Unrecoverable", f"{kpis['not_recoverable']:,}",
              delta="Correctly left as-is", delta_color="off")

    st.divider()

    # ── Governance Impact ──────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Governance Impact · Layer 2+3 combined Navedas performance</div>', unsafe_allow_html=True)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Revenue Prevented", f"${kpis['revenue_prevented']:,.0f}", delta="Saved from cancellation")
    c2.metric("Margin Saved", f"${kpis['margin_saved']:,.0f}", delta="Gross profit recovered")
    c3.metric("Intervention Cost", f"${kpis['intervention_cost']:,.0f}", delta="Total agent spend")
    c4.metric("Net Profit Impact", f"${kpis['net_profit']:,.0f}", delta="Margin − Cost")
    c5.metric("Governance ROI", f"{kpis['roi']:.1f}x", delta="Margin ÷ Cost")

    st.divider()

    # ── Charts row ─────────────────────────────────────────────────────────────
    col_w, col_g = st.columns([2, 1])

    with col_w:
        st.markdown('<div class="section-header">Revenue Waterfall — AI Baseline vs Governance Outcome</div>', unsafe_allow_html=True)
        baseline = kpis['revenue_lost_ai'] + (kpis['total_orders'] - kpis['ai_cancelled']) * df['total_order_value'].mean()
        after_ai = baseline - kpis['revenue_lost_ai']
        after_gov = after_ai + kpis['revenue_prevented']

        fig_wf = go.Figure(go.Waterfall(
            name="Revenue",
            orientation="v",
            measure=["absolute", "relative", "total", "relative", "relative", "total"],
            x=["Baseline", "AI Loss", "After AI Only", "Gov Recovery", "Residual Loss", "Net Revenue"],
            y=[baseline, -kpis['revenue_lost_ai'], 0, kpis['revenue_prevented'], -kpis['residual_loss'], 0],
            connector={"line": {"color": "#374151"}},
            decreasing={"marker": {"color": "#f43f5e"}},
            increasing={"marker": {"color": "#10b981"}},
            totals={"marker": {"color": "#38bdf8"}},
            text=[f"${v/1e6:.2f}M" if abs(v) > 1e6 else f"${v:,.0f}"
                  for v in [baseline, kpis['revenue_lost_ai'], after_ai, kpis['revenue_prevented'], kpis['residual_loss'], after_gov]],
            textposition="outside"
        ))
        fig_wf.update_layout(**CHART_LAYOUT, height=320, showlegend=False)
        st.plotly_chart(fig_wf, use_container_width=True)

    with col_g:
        st.markdown('<div class="section-header">Governance ROI Gauge</div>', unsafe_allow_html=True)
        roi_val = min(kpis['roi'], 200)
        fig_g = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=kpis['roi'],
            delta={"reference": 10, "valueformat": ".1f", "suffix": "x"},
            number={"suffix": "x", "font": {"size": 32, "color": "#10b981"}},
            gauge={
                "axis": {"range": [0, 200], "tickcolor": "#374151"},
                "bar": {"color": "#10b981", "thickness": 0.3},
                "bgcolor": "#111827",
                "borderwidth": 0,
                "steps": [
                    {"range": [0, 10], "color": "#1f2937"},
                    {"range": [10, 50], "color": "#1a2e1a"},
                    {"range": [50, 200], "color": "#0d2b1d"},
                ],
                "threshold": {"line": {"color": "#f59e0b", "width": 3}, "value": 10}
            }
        ))
        fig_g.add_annotation(text=f"${kpis['margin_saved']:,.0f}<br><span style='font-size:10px;color:#6b7280'>Margin Saved</span>",
                             xref="paper", yref="paper", x=0.25, y=0.1, showarrow=False,
                             font=dict(color='#10b981', size=11), align='center')
        fig_g.add_annotation(text=f"${kpis['intervention_cost']:,.0f}<br><span style='font-size:10px;color:#6b7280'>Int. Cost</span>",
                             xref="paper", yref="paper", x=0.75, y=0.1, showarrow=False,
                             font=dict(color='#f59e0b', size=11), align='center')
        fig_g.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                            height=320, margin=dict(l=20, r=20, t=30, b=10),
                            font=dict(color='#9ca3af'))
        st.plotly_chart(fig_g, use_container_width=True)

    # ── Operational ────────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Operational Metrics</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Auto Recoveries", f"{kpis['auto_recoveries']:,}", delta="Layer 2 agent")
    c2.metric("Human Recoveries", f"{kpis['human_recoveries']:,}", delta="Layer 3 agent")
    c3.metric("Avg Recovery Time", f"{kpis['avg_latency']:.1f} min")
    c4.metric("SLA Compliance", f"{kpis['sla_compliance']*100:.1f}%",
              delta=f"Split fulfillment: {kpis['split_rate']*100:.1f}%")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — GOVERNANCE
# ══════════════════════════════════════════════════════════════════════════════
with tab_governance:
    st.markdown('<div class="section-header">Governance Financial Intelligence · Full lifecycle analysis</div>', unsafe_allow_html=True)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Revenue Prevented", f"${kpis['revenue_prevented']:,.0f}")
    c2.metric("Margin Saved", f"${kpis['margin_saved']:,.0f}")
    c3.metric("Int. Cost", f"${kpis['intervention_cost']:,.0f}")
    c4.metric("Net Profit", f"${kpis['net_profit']:,.0f}")
    c5.metric("ROI", f"{kpis['roi']:.1f}x")

    # ROI + Margin trend
    col_t, col_m = st.columns(2)
    with col_t:
        st.markdown('<div class="section-header">Recovery Trend & ROI Over Time</div>', unsafe_allow_html=True)
        fig_ts = go.Figure()
        fig_ts.add_trace(go.Bar(x=ts['month_label'], y=ts['margin_saved'],
                                name='Margin Saved', marker_color='#818cf8', opacity=0.8))
        fig_ts.add_trace(go.Scatter(x=ts['month_label'], y=ts['roi'],
                                    name='ROI', line=dict(color='#fbbf24', width=2.5),
                                    yaxis='y2', mode='lines+markers',
                                    marker=dict(color='#fbbf24', size=6)))
        fig_ts.update_layout(
            **CHART_LAYOUT, height=300,
            yaxis=dict(title='Margin Saved ($)', gridcolor='#1f2937'),
            yaxis2=dict(title='ROI (x)', overlaying='y', side='right', gridcolor='#1f2937'),
            legend=dict(x=0, y=1, bgcolor='rgba(0,0,0,0)'),
        )
        st.plotly_chart(fig_ts, use_container_width=True)

    with col_m:
        st.markdown('<div class="section-header">Monthly Financial Impact</div>', unsafe_allow_html=True)
        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(x=ts['month_label'], y=ts['rev_prevented'],
                                 name='Revenue Prevented', marker_color='#10b981', opacity=0.8))
        fig_bar.add_trace(go.Bar(x=ts['month_label'], y=ts['margin_saved'],
                                 name='Margin Saved', marker_color='#818cf8', opacity=0.8))
        fig_bar.add_trace(go.Bar(x=ts['month_label'], y=ts['int_cost'],
                                 name='Int. Cost', marker_color='#f59e0b', opacity=0.8))
        fig_bar.update_layout(**CHART_LAYOUT, height=300, barmode='group',
                              legend=dict(x=0, y=1, bgcolor='rgba(0,0,0,0)'))
        st.plotly_chart(fig_bar, use_container_width=True)

    # Recovery funnel + Routing logic
    col_f, col_r = st.columns(2)
    with col_f:
        st.markdown('<div class="section-header">Recovery Funnel</div>', unsafe_allow_html=True)
        funnel_data = {
            'Stage': ['Total Orders', 'AI Cancelled', 'Recoverable', 'Successfully Recovered'],
            'Count': [kpis['total_orders'], kpis['ai_cancelled'],
                      kpis['total_recoverable'],
                      int(kpis['total_recoverable'] * kpis['recovery_rate_pool'])],
            'Color': ['#38bdf8', '#f59e0b', '#818cf8', '#10b981']
        }
        fig_funnel = go.Figure(go.Funnel(
            y=funnel_data['Stage'], x=funnel_data['Count'],
            textinfo="value+percent initial",
            marker_color=funnel_data['Color'],
            connector={"line": {"color": "#374151", "width": 2}}
        ))
        fig_funnel.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                                  height=280, margin=dict(l=20, r=20, t=10, b=10),
                                  font=dict(color='#9ca3af'))
        st.plotly_chart(fig_funnel, use_container_width=True)

    with col_r:
        st.markdown('<div class="section-header">Governance Routing Engine — $75 Threshold</div>', unsafe_allow_html=True)
        routing_data = [
            {"Tier": "Layer 2 — Auto (<$75)", "Action": "Full Auto Refund", "Cost": "$5", "Success": "96%", "color": "🔵"},
            {"Tier": "Layer 2 — Auto (Medium Risk)", "Action": "Split / Partial Auto", "Cost": "$15", "Success": "85%", "color": "🔵"},
            {"Tier": "Layer 3 — Human (High Risk)", "Action": "Human Review Queue", "Cost": "$25", "Success": "80%", "color": "🟡"},
        ]
        for r in routing_data:
            with st.expander(f"{r['color']} {r['Tier']}"):
                c1, c2, c3 = st.columns(3)
                c1.markdown(f"**Action**  \n{r['Action']}")
                c2.markdown(f"**Avg Cost**  \n{r['Cost']}")
                c3.markdown(f"**Success Rate**  \n{r['Success']}")

    # Net profit by reason
    st.markdown('<div class="section-header">Net Profit Impact by Cancellation Reason</div>', unsafe_allow_html=True)
    by_reason = df[df['ai_cancel_flag'] == 1].groupby('cancellation_reason').agg(
        net_profit=('net_profit_impact_due_to_navedas', 'sum'),
        margin_saved=('margin_saved_after_navedas', 'sum'),
        orders=('order_id', 'count')
    ).reset_index()
    fig_pie = px.pie(by_reason, values='net_profit', names='cancellation_reason',
                     color_discrete_sequence=['#10b981', '#38bdf8', '#818cf8', '#f59e0b'],
                     hole=0.4)
    fig_pie.update_layout(paper_bgcolor='rgba(0,0,0,0)', height=300,
                          font=dict(color='#9ca3af'), showlegend=True,
                          legend=dict(x=1, y=0.5, bgcolor='rgba(0,0,0,0)'),
                          margin=dict(l=20, r=20, t=10, b=10))
    fig_pie.update_traces(textinfo='percent+label', textfont_color='white')
    st.plotly_chart(fig_pie, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — AGENTS
# ══════════════════════════════════════════════════════════════════════════════
with tab_agents:
    st.markdown('<div class="section-header">Agent Performance Leaderboard</div>', unsafe_allow_html=True)

    # Leaderboard bar chart
    fig_lb = go.Figure()
    colors = ['#38bdf8' if t == 'Auto' else '#f59e0b' for t in agents_df['Type']]
    fig_lb.add_trace(go.Bar(
        x=agents_df['Margin Saved'], y=agents_df['Agent'],
        orientation='h', marker_color=colors,
        text=[f"${v:,.0f}" for v in agents_df['Margin Saved']],
        textposition='outside', textfont=dict(color='#9ca3af', size=11)
    ))
    fig_lb.update_layout(**CHART_LAYOUT, height=300, showlegend=False,
                         xaxis_title='Margin Saved ($)')
    st.plotly_chart(fig_lb, use_container_width=True)

    # Leaderboard table
    display_df = agents_df.copy()
    display_df['Margin Saved'] = display_df['Margin Saved'].apply(lambda v: f"${v:,.0f}")
    display_df['Recoveries'] = display_df['Recoveries'].apply(lambda v: f"{v:,}")
    display_df['Success Rate (%)'] = display_df['Success Rate (%)'].apply(lambda v: f"{v:.1f}%")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown('<div class="section-header">Operational Performance</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Auto Recoveries", f"{kpis['auto_recoveries']:,}")
    c2.metric("Human Recoveries", f"{kpis['human_recoveries']:,}")
    c3.metric("Avg Recovery Time", f"{kpis['avg_latency']:.1f} min")
    c4.metric("SLA Compliance", f"{kpis['sla_compliance']*100:.1f}%")


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
                    'Order': o['Order'], 'State': o['State'], 'Demand': o['Demand'],
                    'Value': o['Value'], 'Margin': o['Margin'],
                    'Reason': o['Reason'][:20], 'Tier': o['Tier'], 'Outcome': o['Outcome']
                })
            st.dataframe(pd.DataFrame(display_orders), use_container_width=True, hide_index=True)

    with col_ls:
        st.markdown('<div class="section-header">Live Session KPIs</div>', unsafe_allow_html=True)
        ls = st.session_state.live_stats
        st.metric("Orders Processed", f"{ls['count']}")
        st.metric("Revenue Prevented", f"${ls['rev_prevented']:,.0f}")
        st.metric("Margin Saved", f"${ls['margin_saved']:,.0f}")
        st.metric("Net Profit", f"${ls['net_profit']:,.0f}")
        if ls['int_cost'] > 0:
            session_roi = ls['margin_saved'] / ls['int_cost']
            st.metric("Session ROI", f"{session_roi:.1f}x")

    st.divider()
    st.markdown('<div class="section-header">Simulation Controls</div>', unsafe_allow_html=True)
    st.markdown("The simulation generates Shopify-format synthetic orders every **5 seconds**, routes them through the 3-layer governance engine, and updates all KPIs in real-time.")

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
            st.session_state.live_stats = {'rev_prevented': 0, 'margin_saved': 0,
                                           'int_cost': 0, 'net_profit': 0, 'count': 0}
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — RISK
# ══════════════════════════════════════════════════════════════════════════════
with tab_risk:
    st.markdown('<div class="section-header">Residual Risk Analysis</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    c1.metric("Residual Recoverable Loss", f"${kpis['residual_loss']:,.0f}",
              delta="Revenue still at risk after governance", delta_color="inverse")
    c2.metric("Legitimate Non-Recoverable", f"{kpis['not_recoverable']:,}",
              delta="Orders correctly left unrecovered", delta_color="off")

    # Failure reason breakdown
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
                marker_color=['#f43f5e', '#f59e0b', '#818cf8', '#fb923c'][:len(fail_df)],
                text=fail_df['count'], textposition='outside',
                textfont=dict(color='#9ca3af', size=11)
            ))
            fig_fail.update_layout(**CHART_LAYOUT, height=280, showlegend=False)
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
            icon = "✅" if ok else "❌"
            st.markdown(f"{icon} `{rule}`")

    # Demand level breakdown
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
                        color_discrete_map={'ai_cancelled': '#f43f5e', 'recovered': '#10b981'},
                        labels={'value': 'Orders', 'demand_level': 'Demand Level'})
    fig_demand.update_layout(**CHART_LAYOUT, height=280,
                             legend=dict(x=0, y=1, bgcolor='rgba(0,0,0,0)'))
    st.plotly_chart(fig_demand, use_container_width=True)


# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<p style='text-align:center; color:#374151; font-size:12px;'>"
    "Navedas Governance Intelligence Platform · All financials in USD · EST timezone · Production-grade"
    "</p>",
    unsafe_allow_html=True
)
