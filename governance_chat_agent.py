"""
governance_chat_agent.py — Navedas NLP Governance Chat Assistant
Keyword-based NLP interpreter that converts natural language questions
into database queries and returns formatted governance insights.

Usage:
    from governance_chat_agent import ask
    response = ask("How much revenue was saved?")
"""
import sqlite3
import os
import tempfile

_DB_FILE = os.path.join(tempfile.gettempdir(), 'navedas_governance.db')

# ── Intent patterns ────────────────────────────────────────────────────────────
INTENT_PATTERNS = {
    'revenue_prevented': [
        'revenue saved', 'revenue prevented', 'how much saved', 'how much revenue',
        'money saved', 'revenue recovery', 'saved today', 'prevented',
    ],
    'roi': [
        'roi', 'return on investment', 'governance roi', 'what is roi',
        'how much roi', 'return', 'efficiency ratio',
    ],
    'recoveries': [
        'how many recoveries', 'recoveries', 'recovered', 'recovery count',
        'orders recovered', 'successful interventions', 'orders saved',
    ],
    'recovery_rate': [
        'recovery rate', 'success rate', 'what percentage recovered',
        'how often', 'effectiveness', 'percentage recovered', 'rate of recovery',
    ],
    'cancellation_rate': [
        'cancellation rate', 'cancel rate', 'ai cancel', 'how many cancelled',
        'cancellations', 'cancelled orders', 'ai cancellation',
    ],
    'health_score': [
        'health score', 'governance health', 'system health', 'ghs',
        'how healthy', 'overall score', 'governance score',
    ],
    'net_profit': [
        'net profit', 'profit impact', 'how much profit', 'bottom line',
        'total profit', 'profit generated',
    ],
    'intervention_cost': [
        'intervention cost', 'how much spent', 'agent cost',
        'total cost', 'spending', 'cost of governance',
    ],
    'agent_stats': [
        'agent performance', 'navedas agent', 'agent stats',
        'how many processed', 'agent processed', 'orders processed by agent',
    ],
    'top_failures': [
        'failure reason', 'why failing', 'top failure', 'failure breakdown',
        'what failed', 'failed interventions', 'failure analysis',
    ],
    'margin_saved': [
        'margin saved', 'gross profit saved', 'profit margin',
        'how much margin', 'margin recovery',
    ],
    'total_orders': [
        'total orders', 'how many orders', 'order count',
        'orders in database', 'total records', 'order volume',
    ],
    'recent_events': [
        'recent events', 'latest interventions', 'timeline',
        'recent activity', 'last events', 'what happened recently',
    ],
    'summary': [
        'summary', 'overview', 'tell me everything', 'full report',
        'give me a summary', 'show all stats', 'dashboard summary', 'all stats',
    ],
    'help': [
        'help', 'what can you do', 'what can i ask', 'commands',
        'examples', 'how to use', 'capabilities', 'questions',
    ],
}


def _detect_intent(question: str) -> str:
    q = question.lower()
    best, best_score = 'unknown', 0
    for intent, patterns in INTENT_PATTERNS.items():
        score = sum(1 for p in patterns if p in q)
        if score > best_score:
            best_score, best = score, intent
    return best if best_score > 0 else 'unknown'


def _fmt(v: float) -> str:
    v = float(v or 0)
    if abs(v) >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    elif abs(v) >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:,.0f}"


def _one(db_path: str, sql: str, params: tuple = ()) -> tuple:
    try:
        conn = sqlite3.connect(db_path)
        row  = conn.execute(sql, params).fetchone()
        conn.close()
        return row or ()
    except Exception:
        return ()


def _many(db_path: str, sql: str, params: tuple = ()) -> list:
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return rows
    except Exception:
        return []


# ── Handlers ───────────────────────────────────────────────────────────────────

def _revenue_prevented(db):
    agent = float((_one(db, "SELECT SUM(revenue_prevented) FROM orders_processed") or (0,))[0] or 0)
    hist  = float((_one(db, "SELECT SUM(revenue_prevented_by_navedas) FROM orders") or (0,))[0] or 0)
    total = agent + hist
    return (
        f"**Revenue Prevented by Navedas Governance**\n\n"
        f"| Source | Amount |\n|---|---|\n"
        f"| Historical dataset | **{_fmt(hist)}** |\n"
        f"| Agent-processed orders | **{_fmt(agent)}** |\n"
        f"| **Total** | **{_fmt(total)}** |\n\n"
        f"Revenue is prevented when a governance intervention successfully "
        f"stops an incorrect AI cancellation."
    )


def _roi(db):
    r  = _one(db, "SELECT SUM(margin_saved), SUM(intervention_cost) FROM orders_processed")
    r2 = _one(db, "SELECT SUM(margin_saved_after_navedas), SUM(intervention_cost) FROM orders")
    a_margin = float((r  or (0, 0))[0] or 0)
    a_cost   = float((r  or (0, 0))[1] or 0)
    h_margin = float((r2 or (0, 0))[0] or 0)
    h_cost   = float((r2 or (0, 0))[1] or 0)
    margin   = a_margin + h_margin
    cost     = a_cost   + h_cost
    roi      = margin / cost if cost > 0 else 0
    return (
        f"**Governance ROI**\n\n"
        f"| Metric | Value |\n|---|---|\n"
        f"| Total Margin Saved | **{_fmt(margin)}** |\n"
        f"| Total Intervention Cost | **{_fmt(cost)}** |\n"
        f"| **ROI** | **{roi:.1f}x** |\n\n"
        f"Every $1 spent on governance returns ${roi:.1f} in margin. "
        f"Formula: ROI = Margin Saved ÷ Intervention Cost"
    )


def _recoveries(db):
    r  = _one(db, "SELECT COUNT(*), SUM(intervention_success) FROM orders_processed")
    r2 = _one(db, "SELECT SUM(recovery_rate_flag), SUM(recoverable_flag) FROM orders")
    a_proc = int((r  or (0, 0))[0] or 0)
    a_rec  = int((r  or (0, 0))[1] or 0)
    h_rec  = int((r2 or (0, 0))[0] or 0)
    h_pool = int((r2 or (0, 0))[1] or 0)
    total_rec  = a_rec + h_rec
    total_pool = a_proc + h_pool
    rate = total_rec / total_pool * 100 if total_pool > 0 else 0
    return (
        f"**Order Recovery Summary**\n\n"
        f"| Source | Recovered |\n|---|---|\n"
        f"| Historical dataset | **{h_rec:,}** |\n"
        f"| Agent-processed | **{a_rec:,}** |\n"
        f"| **Total** | **{total_rec:,}** ({rate:.1f}% recovery rate) |\n\n"
        f"Recovery = governance intervention successfully prevented an AI cancellation."
    )


def _recovery_rate(db):
    r  = _one(db, "SELECT SUM(recovery_rate_flag), SUM(recoverable_flag), SUM(ai_cancel_flag) FROM orders")
    rec   = int((r or (0, 0, 0))[0] or 0)
    pool  = int((r or (0, 0, 0))[1] or 0)
    canc  = int((r or (0, 0, 0))[2] or 0)
    pool_rate = rec / pool * 100  if pool > 0  else 0
    net_rate  = rec / canc * 100  if canc > 0  else 0
    return (
        f"**Recovery Rate Analysis**\n\n"
        f"| Metric | Rate |\n|---|---|\n"
        f"| Recovery Rate (Pool basis) | **{pool_rate:.1f}%** |\n"
        f"| Net Recovery Rate (All AI cancelled) | **{net_rate:.1f}%** |\n"
        f"| Recovered | **{rec:,}** of **{pool:,}** recoverable orders |\n\n"
        f"Pool Rate = Recovered ÷ Recoverable | "
        f"Net Rate = Recovered ÷ All AI-Cancelled"
    )


def _cancellation_rate(db):
    r = _one(db, "SELECT COUNT(*), SUM(ai_cancel_flag) FROM orders")
    total  = int((r or (0, 0))[0] or 0)
    canc   = int((r or (0, 0))[1] or 0)
    rate   = canc / total * 100 if total > 0 else 0
    return (
        f"**AI Cancellation Analysis**\n\n"
        f"| Metric | Value |\n|---|---|\n"
        f"| Total Orders | **{total:,}** |\n"
        f"| AI-Cancelled | **{canc:,}** |\n"
        f"| **Cancel Rate** | **{rate:.1f}%** |\n\n"
        f"These are orders incorrectly flagged by the AI system. "
        f"Navedas governance recovers the majority of these."
    )


def _health_score(db):
    r  = _one(db, "SELECT SUM(recovery_rate_flag), SUM(recoverable_flag) FROM orders")
    r2 = _one(db, "SELECT SUM(avoidable_revenue_loss_after_navedas), SUM(revenue_lost_before_ai_only) FROM orders")
    rec   = int((r  or (0, 0))[0] or 0)
    pool  = int((r  or (0, 0))[1] or 0)
    res   = float((r2 or (0, 1))[0] or 0)
    loss  = float((r2 or (0, 1))[1] or 1)
    rec_eff   = rec / pool if pool > 0 else 0
    res_ctrl  = max(0.0, min(1.0, 1 - (res / loss))) if loss > 0 else 1.0
    sla       = 0.87
    agent_suc = rec_eff
    score = (rec_eff * 0.40 + res_ctrl * 0.30 + sla * 0.20 + agent_suc * 0.10) * 100
    score = max(0.0, min(100.0, score))
    band  = ("Excellent" if score >= 90 else
             "Healthy"   if score >= 75 else
             "Warning"   if score >= 60 else "Critical")
    return (
        f"**Governance Health Score: {score:.1f} — {band}**\n\n"
        f"| Component | Weight | Value |\n|---|---|---|\n"
        f"| Recovery Efficiency | 40% | **{rec_eff*100:.1f}%** |\n"
        f"| Residual Loss Control | 30% | **{res_ctrl*100:.1f}%** |\n"
        f"| SLA Compliance | 20% | **{sla*100:.1f}%** |\n"
        f"| Agent Success Rate | 10% | **{agent_suc*100:.1f}%** |\n\n"
        f"Bands: 90–100 Excellent | 75–89 Healthy | 60–74 Warning | <60 Critical"
    )


def _net_profit(db):
    r  = _one(db, "SELECT SUM(net_profit_impact) FROM orders_processed")
    r2 = _one(db, "SELECT SUM(net_profit_impact_due_to_navedas) FROM orders")
    agent = float((r  or (0,))[0] or 0)
    hist  = float((r2 or (0,))[0] or 0)
    total = agent + hist
    return (
        f"**Net Profit Impact**\n\n"
        f"| Source | Net Profit |\n|---|---|\n"
        f"| Historical dataset | **{_fmt(hist)}** |\n"
        f"| Agent-processed | **{_fmt(agent)}** |\n"
        f"| **Total** | **{_fmt(total)}** |\n\n"
        f"Net Profit = Margin Saved − Intervention Cost"
    )


def _intervention_cost(db):
    r  = _one(db, "SELECT SUM(intervention_cost) FROM orders_processed")
    r2 = _one(db, "SELECT SUM(intervention_cost) FROM orders")
    agent = float((r  or (0,))[0] or 0)
    hist  = float((r2 or (0,))[0] or 0)
    total = agent + hist
    return (
        f"**Total Intervention Cost**\n\n"
        f"| Source | Cost |\n|---|---|\n"
        f"| Historical dataset | **{_fmt(hist)}** |\n"
        f"| Agent-processed | **{_fmt(agent)}** |\n"
        f"| **Total** | **{_fmt(total)}** |\n\n"
        f"Cost per rule: Auto Refund = $5 | Split = $15 | Human = $25 | Retry Payment = $8"
    )


def _agent_stats(db):
    r = _one(db, """SELECT COUNT(*), SUM(intervention_success),
                           SUM(revenue_prevented), SUM(margin_saved), SUM(intervention_cost)
                    FROM orders_processed""")
    if not r or not r[0]:
        return (
            "The Navedas Agent has not processed any orders yet.\n\n"
            "To run: use **+ Feed Orders** then **Run Agent** in the sidebar."
        )
    total, rec, rev, margin, cost = r
    total  = int(total  or 0)
    rec    = int(rec    or 0)
    rev    = float(rev    or 0)
    margin = float(margin or 0)
    cost   = float(cost   or 0)
    rate   = rec / total * 100 if total > 0 else 0
    roi    = margin / cost if cost > 0 else 0
    return (
        f"**Navedas Agent Performance**\n\n"
        f"| Metric | Value |\n|---|---|\n"
        f"| Orders Processed | **{total:,}** |\n"
        f"| Successfully Recovered | **{rec:,}** ({rate:.1f}%) |\n"
        f"| Revenue Prevented | **{_fmt(rev)}** |\n"
        f"| Margin Saved | **{_fmt(margin)}** |\n"
        f"| Intervention Cost | **{_fmt(cost)}** |\n"
        f"| Agent ROI | **{roi:.1f}x** |\n\n"
        f"Use **+ Feed Orders** → **Run Agent** in the sidebar to process more orders."
    )


def _top_failures(db):
    rows = _many(db, """
        SELECT intervention_failure_reason, COUNT(*) as cnt
        FROM orders
        WHERE intervention_failure_reason NOT IN ('None','Not Recoverable')
          AND intervention_failure_reason IS NOT NULL
        GROUP BY intervention_failure_reason
        ORDER BY cnt DESC LIMIT 5
    """)
    if not rows:
        return "No intervention failure data found."
    lines = ["**Top Intervention Failure Reasons**\n\n| Rank | Reason | Count |\n|---|---|---|"]
    for i, (reason, count) in enumerate(rows, 1):
        lines.append(f"| {i} | {reason} | **{count:,}** |")
    lines.append("\nThese are orders where governance intervention was attempted but unsuccessful.")
    return "\n".join(lines)


def _margin_saved(db):
    r  = _one(db, "SELECT SUM(margin_saved) FROM orders_processed")
    r2 = _one(db, "SELECT SUM(margin_saved_after_navedas) FROM orders")
    agent = float((r  or (0,))[0] or 0)
    hist  = float((r2 or (0,))[0] or 0)
    total = agent + hist
    return (
        f"**Margin Saved by Governance**\n\n"
        f"| Source | Margin Saved |\n|---|---|\n"
        f"| Historical dataset | **{_fmt(hist)}** |\n"
        f"| Agent-processed | **{_fmt(agent)}** |\n"
        f"| **Total** | **{_fmt(total)}** |\n\n"
        f"Margin Saved = Order Value × Margin % for each successfully recovered order."
    )


def _total_orders(db):
    r1 = _one(db, "SELECT COUNT(*) FROM orders")
    r2 = _one(db, "SELECT COUNT(*), SUM(processed_flag) FROM orders_feed")
    r3 = _one(db, "SELECT COUNT(*) FROM orders_processed")
    hist      = int((r1 or (0,))[0] or 0)
    feed_tot  = int((r2 or (0, 0))[0] or 0)
    feed_proc = int((r2 or (0, 0))[1] or 0)
    agent_out = int((r3 or (0,))[0] or 0)
    return (
        f"**Order Counts Across System**\n\n"
        f"| Table | Count |\n|---|---|\n"
        f"| orders (historical seed) | **{hist:,}** |\n"
        f"| orders_feed (synthetic) | **{feed_tot:,}** (processed: {feed_proc:,}) |\n"
        f"| orders_processed (agent output) | **{agent_out:,}** |\n"
        f"| **Total in system** | **{hist + feed_tot:,}** |"
    )


def _recent_events(db):
    rows = _many(db, """
        SELECT il.order_id, il.action_taken, il.agent_type,
               il.intervention_time, il.intervention_result,
               op.revenue_prevented
        FROM intervention_log il
        LEFT JOIN orders_processed op ON il.order_id = op.order_id
        ORDER BY il.intervention_id DESC LIMIT 8
    """)
    if not rows:
        return (
            "No recent intervention events found.\n\n"
            "Use **+ Feed Orders** → **Run Agent** in the sidebar to generate events."
        )
    lines = ["**Recent Agent Interventions**\n"]
    for order_id, action, agent, ts, result, rev in rows:
        icon   = "✅" if result == "SUCCESS" else "❌"
        rev_str = f" → {_fmt(float(rev or 0))}" if rev else ""
        time_str = ts[:19] if ts else "N/A"
        lines.append(f"{icon} **{order_id}** | {action} ({agent}){rev_str} | _{time_str}_")
    return "\n".join(lines)


def _summary(db):
    r  = _one(db, """SELECT COUNT(*), SUM(ai_cancel_flag),
                            SUM(recovery_rate_flag), SUM(recoverable_flag)
                     FROM orders""")
    r2 = _one(db, """SELECT SUM(revenue_prevented_by_navedas),
                            SUM(margin_saved_after_navedas),
                            SUM(intervention_cost),
                            SUM(net_profit_impact_due_to_navedas)
                     FROM orders""")
    total   = int(float((r  or (0,)*4)[0] or 0))
    canc    = int(float((r  or (0,)*4)[1] or 0))
    rec     = int(float((r  or (0,)*4)[2] or 0))
    pool    = int(float((r  or (0,)*4)[3] or 0))
    rev     = float((r2 or (0,)*4)[0] or 0)
    margin  = float((r2 or (0,)*4)[1] or 0)
    cost    = float((r2 or (0,)*4)[2] or 0)
    profit  = float((r2 or (0,)*4)[3] or 0)
    roi     = margin / cost if cost > 0 else 0
    c_rate  = canc / total * 100 if total > 0 else 0
    r_rate  = rec  / pool  * 100 if pool  > 0 else 0
    return (
        f"**Navedas GIP — Dashboard Summary**\n\n"
        f"| Metric | Value |\n|---|---|\n"
        f"| Total Orders | **{total:,}** |\n"
        f"| AI Cancel Rate | **{c_rate:.1f}%** ({canc:,} orders) |\n"
        f"| Recovery Rate | **{r_rate:.1f}%** ({rec:,} of {pool:,}) |\n"
        f"| Revenue Prevented | **{_fmt(rev)}** |\n"
        f"| Margin Saved | **{_fmt(margin)}** |\n"
        f"| Intervention Cost | **{_fmt(cost)}** |\n"
        f"| Net Profit | **{_fmt(profit)}** |\n"
        f"| **Governance ROI** | **{roi:.1f}x** |\n\n"
        f"Ask me about any specific metric for a deeper breakdown!"
    )


def _help():
    return (
        "**Navedas Governance Chat Assistant**\n\n"
        "Ask me anything about your governance platform. Examples:\n\n"
        "| Question | What I'll show |\n|---|---|\n"
        "| *How much revenue was prevented?* | Revenue recovery totals |\n"
        "| *What is the governance ROI?* | ROI breakdown |\n"
        "| *How many orders were recovered?* | Recovery counts |\n"
        "| *What is the recovery rate?* | Pool + net recovery % |\n"
        "| *What is the AI cancellation rate?* | Cancel analysis |\n"
        "| *What is the health score?* | GHS breakdown |\n"
        "| *How much net profit was generated?* | Profit figures |\n"
        "| *What is the total intervention cost?* | Cost by source |\n"
        "| *How is the agent performing?* | Agent stats |\n"
        "| *What are the top failure reasons?* | Failure analysis |\n"
        "| *How much margin was saved?* | Margin figures |\n"
        "| *How many total orders?* | All table counts |\n"
        "| *Show recent interventions* | Event timeline |\n"
        "| *Give me a summary* | Full dashboard overview |"
    )


_HANDLERS = {
    'revenue_prevented': _revenue_prevented,
    'roi':               _roi,
    'recoveries':        _recoveries,
    'recovery_rate':     _recovery_rate,
    'cancellation_rate': _cancellation_rate,
    'health_score':      _health_score,
    'net_profit':        _net_profit,
    'intervention_cost': _intervention_cost,
    'agent_stats':       _agent_stats,
    'top_failures':      _top_failures,
    'margin_saved':      _margin_saved,
    'total_orders':      _total_orders,
    'recent_events':     _recent_events,
    'summary':           _summary,
}


def ask(question: str, db_path: str = _DB_FILE) -> str:
    """
    Main entry point.
    Takes a natural language question, detects intent,
    queries DB, returns a formatted markdown answer.
    """
    if not question or not question.strip():
        return "Please type a question about the governance platform."

    intent = _detect_intent(question)

    if intent == 'help':
        return _help()

    if intent == 'unknown':
        return (
            "I'm not sure what you're asking. Try questions like:\n\n"
            "- *How much revenue was saved?*\n"
            "- *What is the ROI?*\n"
            "- *What is the health score?*\n\n"
            "Type **help** to see all supported questions."
        )

    handler = _HANDLERS.get(intent)
    if handler:
        try:
            return handler(db_path)
        except Exception as e:
            return f"Error retrieving data: {str(e)}\n\nThe database may not have data yet."

    return "I couldn't find data for that question. Type **help** for examples."
