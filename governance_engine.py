"""
governance_engine.py — Navedas Governance Decision Engine
Applies 4 rules to each order, returns intervention results.
All values in USD.
"""
import random
from datetime import datetime


RULE_COSTS = {
    'Auto Refund':          5.0,
    'Split Fulfillment':   15.0,
    'Human Agent':         25.0,
    'Retry Payment':        8.0,
    'No Intervention':      0.0,
}

SUCCESS_RATES = {
    'Auto Refund':        0.96,
    'Split Fulfillment':  0.85,
    'Human Agent':        0.80,
    'Retry Payment':      0.72,
    'No Intervention':    0.00,
}


def apply_governance_rules(order: dict) -> dict:
    """
    Apply governance rules to a single order dict.
    Expected keys: order_value, margin_percent, ai_cancel_flag,
                   cancellation_reason, vendor_split_possible
    Returns enriched dict with intervention fields.
    """
    order_value        = float(order.get('order_value', 0))
    margin_percent     = float(order.get('margin_percent', 0))
    ai_cancel_flag     = int(order.get('ai_cancel_flag', 0))
    cancellation_reason= order.get('cancellation_reason', '')
    vendor_split       = bool(order.get('vendor_split_possible', False))

    # Default: no intervention
    intervention_type    = 'No Intervention'
    intervention_success = 0
    revenue_prevented    = 0.0
    margin_saved         = 0.0
    intervention_cost    = 0.0
    net_profit_impact    = 0.0

    if not ai_cancel_flag:
        # Order was not AI-cancelled — no intervention needed
        return _build_result(
            order, 'No Intervention', 0, 0.0, 0.0, 0.0, 0.0
        )

    # ── Rule 1: Auto Refund if order_value < $75 ──────────────────────────────
    if order_value < 75:
        intervention_type = 'Auto Refund'

    # ── Rule 2: Split Fulfillment if vendor split possible ────────────────────
    elif vendor_split or cancellation_reason == 'Vendor Split Possible':
        intervention_type = 'Split Fulfillment'

    # ── Rule 3: Route to Human Agent if high-margin or high-value ─────────────
    elif margin_percent > 0.40 or order_value > 3000:
        intervention_type = 'Human Agent'

    # ── Rule 4: Retry Payment if payment expired ──────────────────────────────
    elif cancellation_reason == 'Payment Expired':
        intervention_type = 'Retry Payment'

    # Default fallback: Auto Refund for remaining AI-cancelled orders
    else:
        intervention_type = 'Auto Refund'

    # Evaluate success
    success_prob         = SUCCESS_RATES[intervention_type]
    intervention_success = 1 if random.random() < success_prob else 0

    intervention_cost = RULE_COSTS[intervention_type]

    if intervention_success:
        revenue_prevented = order_value
        margin_saved      = order_value * margin_percent
    else:
        revenue_prevented = 0.0
        margin_saved      = 0.0

    net_profit_impact = margin_saved - intervention_cost

    return _build_result(
        order, intervention_type, intervention_success,
        revenue_prevented, margin_saved, intervention_cost, net_profit_impact
    )


def _build_result(order, intervention_type, success,
                  rev_prevented, margin_saved, int_cost, net_profit):
    return {
        'order_id':            order.get('order_id', ''),
        'order_value':         float(order.get('order_value', 0)),
        'margin_percent':      float(order.get('margin_percent', 0)),
        'ai_cancel_flag':      int(order.get('ai_cancel_flag', 0)),
        'cancellation_reason': order.get('cancellation_reason', ''),
        'vendor_split_possible': bool(order.get('vendor_split_possible', False)),
        'intervention_type':   intervention_type,
        'intervention_success': success,
        'revenue_prevented':   rev_prevented,
        'margin_saved':        margin_saved,
        'intervention_cost':   int_cost,
        'net_profit_impact':   net_profit,
        'agent_type':          'Auto' if intervention_type in ('Auto Refund', 'Retry Payment')
                               else ('Human' if intervention_type == 'Human Agent' else 'Hybrid'),
        'timestamp':           datetime.utcnow().isoformat(),
    }


def compute_governance_health_score(
    recovery_rate: float,
    residual_loss: float,
    ai_loss: float,
    sla_compliance: float,
    successful_interventions: int,
    total_interventions: int,
) -> dict:
    """
    Governance Health Score (0–100)

    Components:
      Recovery Efficiency   40% — recovery_rate
      Residual Loss Control 30% — 1 - (residual_loss / ai_loss)
      SLA Compliance        20% — sla_compliance_rate
      Agent Success Rate    10% — successful / total interventions

    Returns dict with score and band label.
    """
    recovery_eff  = float(recovery_rate)
    residual_ctrl = 1.0 - (residual_loss / ai_loss) if ai_loss > 0 else 1.0
    residual_ctrl = max(0.0, min(1.0, residual_ctrl))
    sla_comp      = float(sla_compliance)
    agent_success = (successful_interventions / total_interventions
                     if total_interventions > 0 else 0.0)

    score = (
        recovery_eff  * 0.40 +
        residual_ctrl * 0.30 +
        sla_comp      * 0.20 +
        agent_success * 0.10
    ) * 100

    score = max(0.0, min(100.0, score))

    if score >= 90:
        band, color = 'Excellent', '#059669'
    elif score >= 75:
        band, color = 'Healthy',   '#2563eb'
    elif score >= 60:
        band, color = 'Warning',   '#d97706'
    else:
        band, color = 'Critical',  '#e11d48'

    return {'score': round(score, 1), 'band': band, 'color': color}
