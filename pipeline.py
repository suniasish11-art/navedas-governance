"""pipeline.py — Data loading + KPI computation for Navedas Governance Platform"""
import pandas as pd
import numpy as np
import random
import io
import datetime
import sqlite3
import os
import tempfile


def _ensure_extended_schema(conn: sqlite3.Connection) -> None:
    """Create orders_feed / orders_processed / intervention_log if absent."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS orders_feed (
            order_id TEXT PRIMARY KEY, order_value REAL, margin_percent REAL,
            ai_cancel_flag INTEGER DEFAULT 0, cancellation_reason TEXT,
            vendor_split_possible INTEGER DEFAULT 0,
            created_at TEXT, processed_flag INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS orders_processed (
            id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT,
            intervention_type TEXT, intervention_success INTEGER,
            revenue_prevented REAL, margin_saved REAL,
            intervention_cost REAL, net_profit_impact REAL,
            agent_type TEXT, timestamp TEXT
        );
        CREATE TABLE IF NOT EXISTS intervention_log (
            intervention_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT, action_taken TEXT, agent_type TEXT,
            intervention_time TEXT, intervention_result TEXT
        );
    """)
    conn.commit()

US_STATES = ['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI',
             'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI']
CANCELLATION_REASONS = [
    'Payment Expired', 'Vendor Split Possible',
    'Stock Sync Delay', 'AI Logic Gap - SKU Mapping'
]
NUMERIC_COLS = [
    'unit_price', 'quantity', 'total_order_value', 'margin_percent',
    'revenue_lost_before_ai_only', 'revenue_prevented_by_navedas',
    'avoidable_revenue_loss_after_navedas', 'profit_lost_before_ai_only',
    'profit_lost_after_navedas', 'margin_saved_after_navedas',
    'intervention_cost', 'net_profit_impact_due_to_navedas'
]
INT_COLS = ['ai_cancel_flag', 'recoverable_flag', 'intervention_attempted_by_navedas',
            'intervention_success', 'recovery_rate_flag']


def load_data(file_content: str) -> pd.DataFrame:
    """Parse CSV string → clean enriched DataFrame."""
    content = file_content.lstrip('\n\r')
    df = pd.read_csv(io.StringIO(content))
    df = df.dropna(subset=['order_id'])
    df = df[df['order_id'].astype(str).str.strip() != '']

    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # US enrichment
    np.random.seed(42)
    df['customer_state'] = np.random.choice(US_STATES, size=len(df))
    df['payment_auth_status'] = df.apply(_payment_auth, axis=1)
    df['shopify_id'] = df['order_id'].str.replace('ORD-', '#', regex=False)
    df['governance_tier'] = df.apply(_governance_tier, axis=1)
    df['refund_type'] = df.apply(_refund_type, axis=1)

    df['order_date'] = pd.to_datetime(df['order_date'], format='%d-%m-%Y', errors='coerce')
    df['month'] = df['order_date'].dt.to_period('M').astype(str)
    return df


def _payment_auth(row):
    if row.get('cancellation_reason') == 'Payment Expired': return 'Expired'
    if not row.get('ai_cancel_flag'): return 'Authorized'
    return 'Pending'


def _governance_tier(row):
    if not row.get('ai_cancel_flag'): return 'None'
    if row.get('total_order_value', 0) < 75: return 'Auto'
    if not row.get('recoverable_flag'): return 'None'
    if row.get('margin_percent', 0) > 0.40 or row.get('total_order_value', 0) > 3000: return 'Human'
    return 'Auto'


def _refund_type(row):
    if not row.get('intervention_attempted_by_navedas'): return 'N/A'
    if row.get('cancellation_reason') == 'Vendor Split Possible': return 'Split Fulfillment'
    if row.get('total_order_value', 0) < 75: return 'Full'
    return 'Partial'


def compute_kpis(df: pd.DataFrame) -> dict:
    total = len(df)
    ai_cancelled = int(df['ai_cancel_flag'].sum())
    recoverable = int(df['recoverable_flag'].sum())
    recovered = int(df['recovery_rate_flag'].sum())
    rev_lost = float(df['revenue_lost_before_ai_only'].sum())
    rev_prevented = float(df['revenue_prevented_by_navedas'].sum())
    margin_saved = float(df['margin_saved_after_navedas'].sum())
    int_cost = float(df['intervention_cost'].sum())
    net_profit = float(df['net_profit_impact_due_to_navedas'].sum())
    profit_lost = float(df['profit_lost_before_ai_only'].sum())
    residual = float(df['avoidable_revenue_loss_after_navedas'].sum())

    auto_rec = int(df[(df['governance_tier'] == 'Auto') & (df['recovery_rate_flag'] == 1)].shape[0])
    human_rec = int(df[(df['governance_tier'] == 'Human') & (df['recovery_rate_flag'] == 1)].shape[0])
    split_df = df[df['refund_type'] == 'Split Fulfillment']
    split_success = int(split_df[split_df['recovery_rate_flag'] == 1].shape[0])
    split_total = len(split_df)

    return {
        'total_orders': total, 'ai_cancelled': ai_cancelled,
        'ai_cancel_rate': ai_cancelled / total if total > 0 else 0,
        'revenue_lost_ai': rev_lost, 'profit_lost_ai': profit_lost,
        'total_recoverable': recoverable,
        'pct_recoverable': recoverable / ai_cancelled if ai_cancelled > 0 else 0,
        'recovery_rate_pool': recovered / recoverable if recoverable > 0 else 0,
        'recovery_rate_total': recovered / ai_cancelled if ai_cancelled > 0 else 0,
        'not_recoverable': ai_cancelled - recoverable,
        'recovered': recovered,
        'revenue_prevented': rev_prevented, 'margin_saved': margin_saved,
        'intervention_cost': int_cost, 'net_profit': net_profit,
        'roi': margin_saved / int_cost if int_cost > 0 else 0,
        'residual_loss': residual,
        'auto_recoveries': auto_rec, 'human_recoveries': human_rec,
        'avg_latency': 12.4, 'sla_compliance': 0.87,
        'split_total': split_total, 'split_success': split_success,
        'split_rate': split_success / split_total if split_total > 0 else 0,
    }


def compute_time_series(df: pd.DataFrame) -> pd.DataFrame:
    monthly = df.groupby('month').agg(
        total_orders=('order_id', 'count'),
        ai_cancelled=('ai_cancel_flag', 'sum'),
        recovered=('recovery_rate_flag', 'sum'),
        rev_prevented=('revenue_prevented_by_navedas', 'sum'),
        margin_saved=('margin_saved_after_navedas', 'sum'),
        int_cost=('intervention_cost', 'sum'),
    ).reset_index()
    monthly['roi'] = monthly.apply(
        lambda r: r['margin_saved'] / r['int_cost'] if r['int_cost'] > 0 else 0, axis=1)
    monthly['month_label'] = pd.to_datetime(monthly['month']).dt.strftime('%b %y')
    return monthly


def compute_agent_stats(df: pd.DataFrame) -> pd.DataFrame:
    auto_margin = float(df[df['governance_tier'] == 'Auto']['margin_saved_after_navedas'].sum())
    human_margin = float(df[df['governance_tier'] == 'Human']['margin_saved_after_navedas'].sum())
    auto_rec = int(df[(df['governance_tier'] == 'Auto') & (df['recovery_rate_flag'] == 1)].shape[0])
    human_rec = int(df[(df['governance_tier'] == 'Human') & (df['recovery_rate_flag'] == 1)].shape[0])
    np.random.seed(99)
    hw = np.random.dirichlet(np.ones(4))
    aw = np.array([0.4, 0.6])
    agents = [
        ('AutoGov Alpha', 'Auto', int(auto_rec * aw[0]), auto_margin * aw[0], 5.5, 91.2),
        ('AutoGov Beta',  'Auto', int(auto_rec * aw[1]), auto_margin * aw[1], 7.1, 88.4),
        ('Sarah Chen',   'Human', int(human_rec * hw[0]), human_margin * hw[0], 24.3, 82.1),
        ('Marcus Rivera','Human', int(human_rec * hw[1]), human_margin * hw[1], 28.7, 79.4),
        ('Priya Kapoor', 'Human', int(human_rec * hw[2]), human_margin * hw[2], 31.2, 83.6),
        ('Tyler Brooks', 'Human', int(human_rec * hw[3]), human_margin * hw[3], 26.8, 80.9),
    ]
    df_out = pd.DataFrame(agents, columns=['Agent', 'Type', 'Recoveries', 'Margin Saved', 'Avg Latency (min)', 'Success Rate (%)'])
    return df_out.sort_values('Margin Saved', ascending=False).reset_index(drop=True)


_DB_FILE = os.path.join(tempfile.gettempdir(), 'navedas_governance.db')


def load_csv_to_db(csv_content: str) -> str:
    """Parse CSV → clean DataFrame → insert into SQLite. Returns db_path."""
    df = load_data(csv_content)
    # Convert period/datetime back to strings for SQLite storage
    df['order_date'] = df['order_date'].astype(str)
    conn = sqlite3.connect(_DB_FILE)
    df.to_sql('orders', conn, if_exists='replace', index=False)
    _ensure_extended_schema(conn)
    conn.close()
    return _DB_FILE


def load_from_db(db_path: str = _DB_FILE) -> pd.DataFrame:
    """Load orders table from SQLite and return a clean DataFrame."""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql('SELECT * FROM orders', conn)
    conn.close()
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df


def db_exists(db_path: str = _DB_FILE) -> bool:
    """Return True if the DB file exists and has the orders table."""
    if not os.path.exists(db_path):
        return False
    try:
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        _ensure_extended_schema(conn)
        conn.close()
        return count > 0
    except Exception:
        return False


FAILURE_REASONS = [
    'Customer Abandoned', 'Customer Payment Expired',
    'Vendor SLA Delay', 'Late Detection'
]


def generate_dataset(n: int = 5000, seed: int = 42) -> pd.DataFrame:
    """Generate a synthetic ecommerce governance dataset — no CSV required."""
    rng = np.random.default_rng(seed)

    # Date range: Jan 2025 – Jun 2025
    start = datetime.date(2025, 1, 1)
    days  = (datetime.date(2025, 6, 30) - start).days
    order_dates = [start + datetime.timedelta(days=int(d))
                   for d in rng.integers(0, days, n)]

    qty    = rng.integers(1, 5, n).astype(float)
    price  = rng.integers(50, 3500, n).astype(float)
    value  = price * qty
    margin = rng.uniform(0.20, 0.55, n)

    ai_cancel    = rng.random(n) < 0.65
    recoverable  = ai_cancel & (rng.random(n) < 0.75)
    intervention = recoverable.copy()

    tier = np.where(~ai_cancel, 'None',
           np.where(~recoverable, 'None',
           np.where(value < 75, 'Auto',
           np.where((margin > 0.40) | (value > 3000), 'Human', 'Auto'))))

    success_rate = np.where(tier == 'Auto', 0.85, 0.80)
    success = intervention & (rng.random(n) < success_rate)

    cost_per  = np.where(value < 75, 5, np.where(tier == 'Human', 25, 15))
    int_cost  = np.where(intervention, cost_per, 0).astype(float)

    rev_lost     = np.where(ai_cancel, value, 0).astype(float)
    profit_lost  = np.where(ai_cancel, value * margin, 0).astype(float)
    rev_prev     = np.where(success, value, 0).astype(float)
    margin_saved = np.where(success, value * margin, 0).astype(float)
    net_profit   = margin_saved - int_cost
    residual     = np.where(intervention & ~success, value, 0).astype(float)
    profit_after = np.where(success, 0, np.where(ai_cancel, value * margin, 0)).astype(float)

    _fr = rng.choice(FAILURE_REASONS, n)
    failure_reason = np.where(
        intervention & ~success, _fr,
        np.where(~ai_cancel, 'None',
        np.where(~recoverable, 'Not Recoverable', 'None'))
    )
    cancel_reason = np.where(ai_cancel,
                             rng.choice(CANCELLATION_REASONS, n), 'N/A')
    demand = rng.choice(['High', 'Medium', 'Low'], n, p=[0.35, 0.45, 0.20])

    df = pd.DataFrame({
        'order_id':   [f'ORD-{800000+i:06d}' for i in range(n)],
        'order_date': [d.strftime('%d-%m-%Y') for d in order_dates],
        'demand_level': demand,
        'unit_price':   price,
        'quantity':     qty,
        'total_order_value': value,
        'margin_percent':    margin,
        'cancellation_reason': cancel_reason,
        'ai_cancel_flag':    ai_cancel.astype(int),
        'recoverable_flag':  recoverable.astype(int),
        'intervention_attempted_by_navedas': intervention.astype(int),
        'intervention_success':  success.astype(int),
        'recovery_rate_flag':    success.astype(int),
        'revenue_lost_before_ai_only':          rev_lost,
        'revenue_prevented_by_navedas':         rev_prev,
        'avoidable_revenue_loss_after_navedas': residual,
        'profit_lost_before_ai_only':  profit_lost,
        'profit_lost_after_navedas':   profit_after,
        'margin_saved_after_navedas':  margin_saved,
        'intervention_cost':           int_cost,
        'net_profit_impact_due_to_navedas': net_profit,
        'intervention_failure_reason': failure_reason,
    })

    # Same enrichment as load_data
    df['customer_state'] = rng.choice(US_STATES, n)
    df['payment_auth_status'] = df.apply(_payment_auth, axis=1)
    df['shopify_id']   = df['order_id'].str.replace('ORD-', '#', regex=False)
    df['governance_tier'] = tier
    df['refund_type']  = df.apply(_refund_type, axis=1)
    df['order_date']   = pd.to_datetime(df['order_date'], format='%d-%m-%Y')
    df['month']        = df['order_date'].dt.to_period('M').astype(str)

    for col in INT_COLS:
        if col in df.columns:
            df[col] = df[col].astype(int)
    return df


def generate_live_order(counter: int) -> dict:
    reason = random.choice(CANCELLATION_REASONS)
    demand = random.choice(['High', 'Medium', 'Low'])
    state = random.choice(US_STATES)
    qty = random.randint(1, 4)
    price = random.randint(50, 3500)
    value = price * qty
    margin = round(random.uniform(0.20, 0.55), 2)
    ai_cancel = random.random() < 0.65
    recoverable = ai_cancel and random.random() < 0.75

    tier = 'None'
    if ai_cancel and recoverable:
        if value < 75: tier = 'Auto'
        elif margin > 0.40 or value > 3000: tier = 'Human'
        else: tier = 'Auto'

    success = ai_cancel and recoverable and random.random() < 0.85
    rev_prevented = value if success else 0
    ms = value * margin if success else 0
    cost = (5 if value < 75 else 25 if tier == 'Human' else 15) if (ai_cancel and recoverable) else 0

    outcomes = {
        (False, False): ('Fulfilled', '🟢'),
        (True, False):  ('Not Recoverable', '🔴'),
        (True, True):   ('Recovered ✓' if success else 'Failed', '🟢' if success else '🟡'),
    }
    out_label, icon = outcomes.get((bool(ai_cancel), bool(recoverable)), ('Unknown', '⚪'))

    return {
        'Order': f'#LIVE-{counter}', 'State': state, 'Demand': demand,
        'Value': f'${value:,.0f}', 'Margin': f'{margin*100:.0f}%',
        'Reason': reason, 'Tier': tier,
        'Outcome': f'{icon} {out_label}',
        # financial accumulators
        '_rev_prevented': rev_prevented, '_margin_saved': ms,
        '_int_cost': cost, '_net_profit': ms - cost,
        '_residual_loss': value if (recoverable and not success) else 0,
        # count accumulators — used to update live_stats directly
        '_ai_cancelled':    1 if ai_cancel else 0,
        '_recoverable':     1 if recoverable else 0,
        '_not_recoverable': 1 if (ai_cancel and not recoverable) else 0,
        '_recovered':       1 if success else 0,
        '_auto_recovery':   1 if (tier == 'Auto' and success) else 0,
        '_human_recovery':  1 if (tier == 'Human' and success) else 0,
    }
