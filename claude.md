# Navedas Governance Intelligence Platform — Engineering & Product Documentation

## System Overview

Navedas GIP is a production-grade AI order governance platform built on Streamlit.
It monitors ecommerce orders flagged for cancellation by AI systems, applies a
multi-layer governance engine to recover revenue, and presents real-time KPIs on
a SaaS-quality dashboard.

---

## Architecture

```
Synthetic Order Feed (synthetic_feed_generator.py)
        │
        │  inserts rows every few seconds
        ▼
orders_feed table (SQLite)
        │
        │  polled every 15 seconds
        ▼
Navedas Governance Agent (navedas_agent.py)
        │
        │  applies 4 governance rules
        ▼
orders_processed + intervention_log tables (SQLite)
        │
        │  read every 5 seconds (auto-refresh)
        ▼
Governance Intelligence Dashboard (app.py / Streamlit)
```

---

## File Structure

| File | Role |
|---|---|
| `app.py` | Dashboard UI — visualization layer only |
| `pipeline.py` | Data loading, KPI computation, DB helpers |
| `governance_engine.py` | 4-rule decision engine + GHS formula |
| `navedas_agent.py` | Agent loop — polls feed, applies rules, writes results |
| `synthetic_feed_generator.py` | Inserts synthetic orders into orders_feed |
| `data.csv` | 5000-row historical order dataset (seed data) |

---

## Database Schema

### orders (historical seed data)
Original 5000 CSV-imported rows. Used for static KPIs and charts.

### orders_feed
```
order_id            TEXT PK
order_value         REAL
margin_percent      REAL
ai_cancel_flag      INTEGER (0|1)
cancellation_reason TEXT
vendor_split_possible INTEGER (0|1)
created_at          TEXT (ISO8601)
processed_flag      INTEGER (0|1)
```

### orders_processed
```
id                  INTEGER PK AUTOINCREMENT
order_id            TEXT
intervention_type   TEXT
intervention_success INTEGER (0|1)
revenue_prevented   REAL
margin_saved        REAL
intervention_cost   REAL
net_profit_impact   REAL
agent_type          TEXT (Auto|Human|Hybrid)
timestamp           TEXT (ISO8601)
```

### intervention_log
```
intervention_id     INTEGER PK AUTOINCREMENT
order_id            TEXT
action_taken        TEXT
agent_type          TEXT
intervention_time   TEXT (ISO8601)
intervention_result TEXT (SUCCESS|FAILED)
```

---

## Synthetic Order Feed (synthetic_feed_generator.py)

Generates realistic ecommerce orders and inserts them into `orders_feed`.

**Parameters:**
- `batch_size = 5` — orders per insert cycle
- `interval_seconds = 3.0` — seconds between inserts
- `max_orders = 0` — 0 means run forever

**Run standalone:**
```bash
python synthetic_feed_generator.py
```

**Order fields generated:**
- `order_value` — $20–$17,500 (qty × unit price)
- `margin_percent` — 18%–58%
- `ai_cancel_flag` — 65% cancellation rate
- `cancellation_reason` — one of 4 standard reasons
- `vendor_split_possible` — true when reason is Vendor Split

---

## Navedas Governance Agent (navedas_agent.py)

Polls `orders_feed` for unprocessed orders, applies governance rules,
writes results to `orders_processed` and `intervention_log`.

**Cycle:**
1. `SELECT ... WHERE processed_flag = 0 LIMIT 50`
2. Apply `apply_governance_rules()` to each order
3. `INSERT INTO orders_processed`
4. `INSERT INTO intervention_log`
5. `UPDATE orders_feed SET processed_flag = 1`

**Poll interval:** 15 seconds

**Run standalone:**
```bash
python navedas_agent.py
```

**Single cycle (for testing):**
```python
from navedas_agent import run_agent_cycle
summary = run_agent_cycle()
```

---

## Governance Decision Engine (governance_engine.py)

Applies 4 rules in priority order to each order.

### Rule 1 — Auto Refund
**Condition:** `order_value < $75`
**Action:** Full automatic refund
**Cost:** $5 | **Success Rate:** 96%

### Rule 2 — Split Fulfillment
**Condition:** `vendor_split_possible = true` OR `cancellation_reason = "Vendor Split Possible"`
**Action:** Attempt vendor split fulfillment
**Cost:** $15 | **Success Rate:** 85%

### Rule 3 — Human Agent
**Condition:** `margin_percent > 40%` OR `order_value > $3,000`
**Action:** Route to human review queue
**Cost:** $25 | **Success Rate:** 80%

### Rule 4 — Retry Payment
**Condition:** `cancellation_reason = "Payment Expired"`
**Action:** Retry payment authorization
**Cost:** $8 | **Success Rate:** 72%

**Default (fallback):** Auto Refund for any remaining AI-cancelled order

### Outputs per order:
```
revenue_prevented   = order_value if success else 0
margin_saved        = order_value × margin_percent if success else 0
intervention_cost   = rule cost if intervention attempted
net_profit_impact   = margin_saved − intervention_cost
```

---

## Governance Health Score (GHS)

Single 0–100 executive indicator of system governance health.

### Formula
```
GHS = (RecoveryEfficiency  × 0.40)
    + (ResidualLossControl × 0.30)
    + (SLACompliance       × 0.20)
    + (AgentSuccessRate    × 0.10)

RecoveryEfficiency  = recovery_rate (recovered / recoverable)
ResidualLossControl = 1 − (residual_loss / ai_loss)  [clamped 0–1]
SLACompliance       = sla_compliance_rate
AgentSuccessRate    = successful_interventions / total_interventions

Score is clamped to [0, 100]
```

### Score Bands
| Score | Band | Color |
|---|---|---|
| 90–100 | Excellent | Green |
| 75–89 | Healthy | Blue |
| 60–74 | Warning | Amber |
| < 60 | Critical | Red |

---

## Dashboard Layout

### Tab 1 — Overview
- Executive KPI row (Total Orders, Cancel Rate, Revenue Lost AI, GHS)
- Recoverability Analysis (Recoverable, Recovery Rate, Net Rate, Unrecoverable)
- Governance Impact (Revenue Prevented, Margin Saved, Cost, Net Profit, ROI)
- Revenue Waterfall chart
- Governance ROI Gauge
- Governance Health Score Gauge
- Operational Metrics (Auto, Human, Avg Time, SLA)

### Tab 2 — Governance
- Governance Financial Intelligence KPI row
- Recovery Trend & ROI Over Time (bar + line dual axis)
- Monthly Financial Impact (grouped bar)
- Recovery Funnel (5-stage Plotly funnel)
- Governance Routing Engine (4-rule card display)
- Net Profit by Cancellation Reason (donut chart)

### Tab 3 — Agents
- Agent Performance Leaderboard (horizontal bar chart)
- Agent table (sortable)
- Operational Performance KPIs

### Tab 4 — Live Feed
- Real-time order stream table
- Session KPI panel
- Simulation controls (Start / Stop / Clear)
- Manual Order Entry form

### Tab 5 — Risk
- Residual Risk Analysis KPIs
- Failure Reason Breakdown (horizontal bar)
- Financial Integrity Matrix (check list)
- Risk by Demand Level (grouped bar)

### Tab 6 — Agent Intel (NEW)
- Agent DB aggregate stats
- Event Timeline (recent interventions from DB)
- Intervention type distribution (pie + bar)

### Tab 7 — Architecture (NEW)
- Visual architecture diagram (4-node flow)
- Database schema cards
- Governance rules reference
- GHS formula
- Live DB row counts

---

## Governance KPI Definitions

| KPI | Formula |
|---|---|
| AI Cancel Rate | ai_cancelled / total_orders |
| Recovery Rate (Pool) | recovered / recoverable |
| Net Recovery Rate | recovered / ai_cancelled |
| Revenue Prevented | sum(revenue_prevented_by_navedas) |
| Margin Saved | sum(margin_saved_after_navedas) |
| Intervention Cost | sum(intervention_cost) |
| Net Profit Impact | margin_saved − intervention_cost |
| Governance ROI | margin_saved / intervention_cost |
| Residual Loss | sum(avoidable_revenue_loss_after_navedas) |
| Governance Health Score | Weighted formula (see above) |

---

## Live Simulation

Sidebar controls trigger in-session synthetic order generation.

- Generates **3 orders per 2-second cycle** (~90/min)
- Orders are routed through the same governance logic as the engine
- KPIs update across ALL tabs in real-time
- Session data is in `st.session_state` — not persisted to DB
- Use **Clear History** to reset session KPIs

---

## UI Design System

**Primary accent:** `#6C63FF` (indigo/purple)
**Background:** `#F6F8FC` (soft grey-white)

### KPI Card Colors
| Metric type | Background | Accent |
|---|---|---|
| Loss metrics | `#FFE6E6` | `#e11d48` (red) |
| Recovery metrics | `#E6F7EE` | `#059669` (green) |
| Financial metrics | `#E8F5E9` | `#059669` (green) |
| Operational | `#F1F0FF` | `#6C63FF` (purple) |
| Neutral | `#F6F8FC` | `#6b7280` (grey) |
| Warning | `#fffbeb` | `#d97706` (amber) |

**Typography:** Inter (Google Fonts), 300–800 weights
**Border radius:** 12–16px on cards, 8px on buttons
**Box shadow:** `0 2px 8px rgba(0,0,0,0.04)` on cards

---

## Deployment

**Streamlit Cloud:**
- Push to GitHub → auto-deploys
- Set `APP_PASSWORD` in Streamlit Secrets
- Optionally set `ALLOWED_IPS` for IP allowlisting

**Requirements:** See `requirements.txt`

**Local dev:**
```bash
pip install -r requirements.txt
streamlit run app.py
```

---

## Known Constraints

- SQLite is ephemeral on Streamlit Cloud (resets on container restart)
- `data.csv` auto-seeds the DB on first run
- Agent runs in single-process mode within Streamlit (manual trigger via sidebar)
- For production, run `navedas_agent.py` and `synthetic_feed_generator.py` as separate processes pointing to a shared persistent DB
