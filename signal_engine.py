"""
signal_engine.py — Navedas Signal Intelligence Engine
Identifies meaningful governance signals from operational data.

Signals detected:
  1. Revenue Risk Spike        — cancellation rate above baseline
  2. Logic Gap Cluster         — recoverable orders left unrecovered
  3. Governance Health Drift   — GHS below healthy threshold
  4. Recovery Failure Cluster  — agent interventions failing

Each signal includes severity, impact estimate, and root cause.
"""
import datetime
from db import get_conn, SQLITE_PATH


def _q1(db_path: str, sql: str, params: tuple = ()) -> tuple:
    try:
        conn = get_conn(db_path)
        row  = conn.execute(sql, params).fetchone()
        conn.close()
        return row or ()
    except Exception:
        return ()


def _q_many(db_path: str, sql: str, params: tuple = ()) -> list:
    try:
        conn = get_conn(db_path)
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return rows
    except Exception:
        return []


# ── Signal severity helpers ────────────────────────────────────────────────────

_SEV_ORDER = {'CRITICAL': 0, 'WARNING': 1, 'INFO': 2}

def _sev_style(severity: str) -> tuple:
    """Return (icon, color, bg, border) for a severity level."""
    return {
        'CRITICAL': ('🔴', '#e11d48', '#fff1f2', '#fecdd3'),
        'WARNING':  ('🟡', '#d97706', '#fffbeb', '#fde68a'),
        'INFO':     ('🟢', '#059669', '#f0fdf4', '#bbf7d0'),
    }.get(severity, ('⚪', '#6b7280', '#f9fafb', '#e5e7eb'))


# ── Individual signal detectors ────────────────────────────────────────────────

def _signal_revenue_risk_spike(db_path: str, now: str) -> dict | None:
    """
    Signal 1 — Revenue Risk Spike
    Fires when AI cancellation rate is ≥15% above the expected 35% baseline.
    """
    r = _q1(db_path, "SELECT COUNT(*), SUM(ai_cancel_flag) FROM orders")
    total     = int(float((r or (1, 0))[0] or 1))
    cancelled = int(float((r or (1, 0))[1] or 0))
    rate      = cancelled / total
    baseline  = 0.35

    if rate <= baseline * 1.15:
        return None

    excess  = max(0, int(cancelled - baseline * total))
    impact  = excess * 285          # avg USD order value
    pct_over = ((rate / baseline) - 1) * 100
    sev = 'CRITICAL' if rate > baseline * 1.35 else 'WARNING'

    return {
        'signal_type':         'Revenue Risk Spike',
        'severity_level':      sev,
        'detected_timestamp':  now,
        'impact_estimate_usd': impact,
        'primary_root_cause':  (
            f'AI cancellation rate at {rate*100:.1f}% — '
            f'{pct_over:.0f}% above {baseline*100:.0f}% baseline. '
            f'{excess:,} excess cancellations detected.'
        ),
    }


def _signal_logic_gap_cluster(db_path: str, now: str) -> dict | None:
    """
    Signal 2 — Logic Gap Cluster
    Fires when recoverable orders exist but have NOT been recovered.
    Indicates AI cancellation logic gaps that governance can correct.
    """
    r = _q1(db_path,
            "SELECT COUNT(*), SUM(total_order_value) FROM orders "
            "WHERE ai_cancel_flag=1 AND recoverable_flag=1 AND recovery_rate_flag=0")
    gaps    = int(float((r or (0, 0))[0] or 0))
    gap_val = float((r or (0, 0))[1] or 0)

    if gaps < 20:
        return None

    impact = gap_val * 0.30          # avg 30% margin on gap orders
    sev    = 'CRITICAL' if gaps > 150 else 'WARNING'

    # Find top cancellation reasons in logic gap pool
    reasons = _q_many(db_path,
        "SELECT cancellation_reason, COUNT(*) as cnt FROM orders "
        "WHERE ai_cancel_flag=1 AND recoverable_flag=1 AND recovery_rate_flag=0 "
        "GROUP BY cancellation_reason ORDER BY cnt DESC LIMIT 2")
    top_reasons = ', '.join(r[0] for r in reasons) if reasons else 'Multiple reasons'

    return {
        'signal_type':         'Logic Gap Cluster',
        'severity_level':      sev,
        'detected_timestamp':  now,
        'impact_estimate_usd': impact,
        'primary_root_cause':  (
            f'{gaps:,} AI-cancelled recoverable orders remain unrecovered. '
            f'Primary reasons: {top_reasons}. '
            f'Estimated margin at risk: ${impact:,.0f}.'
        ),
    }


def _signal_ghs_drift(db_path: str, now: str) -> dict | None:
    """
    Signal 3 — Governance Health Drift
    Fires when estimated GHS falls below the 'Healthy' threshold of 75.
    """
    r = _q1(db_path,
            "SELECT SUM(recovery_rate_flag), SUM(recoverable_flag), "
            "SUM(avoidable_revenue_loss_after_navedas), "
            "SUM(revenue_lost_before_ai_only) FROM orders")
    rec      = int(float((r or (0, 1, 0, 1))[0] or 0))
    pool     = int(float((r or (0, 1, 0, 1))[1] or 1))
    res_loss = float((r or (0, 1, 0, 1))[2] or 0)
    ai_loss  = float((r or (0, 1, 0, 1))[3] or 1)

    rec_eff   = rec / pool if pool > 0 else 0
    res_ctrl  = max(0.0, min(1.0, 1 - res_loss / ai_loss)) if ai_loss > 0 else 1.0
    ghs_score = (rec_eff * 0.40 + res_ctrl * 0.30 + 0.87 * 0.20 + rec_eff * 0.10) * 100

    if ghs_score >= 75:
        return None

    sev = 'CRITICAL' if ghs_score < 60 else 'WARNING'
    band = 'Critical' if ghs_score < 60 else 'Warning'

    return {
        'signal_type':         'Governance Health Drift',
        'severity_level':      sev,
        'detected_timestamp':  now,
        'impact_estimate_usd': res_loss,
        'primary_root_cause':  (
            f'Governance Health Score at {ghs_score:.0f} ({band}) — '
            f'below Healthy threshold (75+). '
            f'Recovery efficiency: {rec_eff*100:.1f}%. '
            f'Residual loss control: {res_ctrl*100:.1f}%.'
        ),
    }


def _signal_recovery_failure_cluster(db_path: str, now: str) -> dict | None:
    """
    Signal 4 — Recovery Failure Cluster
    Fires when agent-processed interventions have a high failure count.
    """
    r = _q1(db_path,
            "SELECT COUNT(*), SUM(intervention_cost), "
            "SUM(revenue_prevented) FROM orders_processed "
            "WHERE intervention_success=0")
    failed      = int(float((r or (0, 0, 0))[0] or 0))
    wasted_cost = float((r or (0, 0, 0))[1] or 0)

    if failed < 5:
        return None

    total_r = _q1(db_path, "SELECT COUNT(*) FROM orders_processed")
    total_p = int(float((total_r or (1,))[0] or 1))
    fail_rate = failed / total_p
    sev = 'CRITICAL' if fail_rate > 0.40 else 'WARNING'

    return {
        'signal_type':         'Recovery Failure Cluster',
        'severity_level':      sev,
        'detected_timestamp':  now,
        'impact_estimate_usd': wasted_cost,
        'primary_root_cause':  (
            f'{failed} agent interventions failed ({fail_rate*100:.0f}% failure rate). '
            f'${wasted_cost:,.0f} in intervention costs unrecovered. '
            f'Review governance routing rules.'
        ),
    }


# ── Public API ─────────────────────────────────────────────────────────────────

def detect_signals(db_path: str = SQLITE_PATH) -> list:
    """
    Run all signal detectors.
    Returns list of active signal dicts, sorted CRITICAL → WARNING → INFO.
    Each dict contains:
        signal_type, severity_level, detected_timestamp,
        impact_estimate_usd, primary_root_cause,
        icon, color, bg, border  (display helpers)
    """
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')

    detectors = [
        _signal_revenue_risk_spike,
        _signal_logic_gap_cluster,
        _signal_ghs_drift,
        _signal_recovery_failure_cluster,
    ]

    signals = []
    for fn in detectors:
        try:
            sig = fn(db_path, now)
            if sig:
                signals.append(sig)
        except Exception:
            pass

    if not signals:
        signals.append({
            'signal_type':         'System Nominal',
            'severity_level':      'INFO',
            'detected_timestamp':  now,
            'impact_estimate_usd': 0,
            'primary_root_cause':  (
                'All governance metrics within expected parameters. '
                'No anomalies detected.'
            ),
        })

    # Sort by severity
    signals.sort(key=lambda s: _SEV_ORDER.get(s['severity_level'], 3))

    # Attach display helpers
    for s in signals:
        icon, color, bg, border = _sev_style(s['severity_level'])
        s.update({'icon': icon, 'color': color, 'bg': bg, 'border': border})

    return signals


def get_signal_summary(db_path: str = SQLITE_PATH) -> str:
    """Return a short text summary of active signals — used by chat assistant."""
    signals = detect_signals(db_path)
    if len(signals) == 1 and signals[0]['signal_type'] == 'System Nominal':
        return "**No active governance signals.** All metrics within parameters."

    lines = [f"**{len(signals)} Active Governance Signal(s)**\n"]
    for s in signals:
        lines.append(
            f"- {s['icon']} **{s['signal_type']}** [{s['severity_level']}]  \n"
            f"  {s['primary_root_cause']}  \n"
            f"  Impact estimate: ${s['impact_estimate_usd']:,.0f}"
        )
    return "\n".join(lines)


def compute_ghs_trend(df) -> list:
    """
    Compute monthly Governance Health Score from the DataFrame.
    Returns list of {month, score, band}.
    """
    results = []
    try:
        for month, grp in df.groupby('month'):
            rec    = int(grp['recovery_rate_flag'].sum())
            pool   = int(grp['recoverable_flag'].sum())
            res    = float(grp['avoidable_revenue_loss_after_navedas'].sum())
            ai_l   = float(grp['revenue_lost_before_ai_only'].sum())
            rec_eff  = rec / pool if pool > 0 else 0
            res_ctrl = max(0.0, min(1.0, 1 - res / ai_l)) if ai_l > 0 else 1.0
            score = (rec_eff * 0.40 + res_ctrl * 0.30 + 0.87 * 0.20 + rec_eff * 0.10) * 100
            score = round(max(0.0, min(100.0, score)), 1)
            band  = ('Excellent' if score >= 90 else
                     'Healthy'   if score >= 75 else
                     'Warning'   if score >= 60 else 'Critical')
            results.append({'month': month, 'score': score, 'band': band})
    except Exception:
        pass
    return sorted(results, key=lambda x: x['month'])
