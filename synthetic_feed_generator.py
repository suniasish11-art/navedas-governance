"""
synthetic_feed_generator.py — Navedas Static Order Feed
Inserts synthetic ecommerce orders into orders_feed table.
Can be run as a standalone process or called from navedas_agent.py.
"""
import random
import time
import datetime
import os
from db import get_conn, SQLITE_PATH

# ── DB path ────────────────────────────────────────────────────────────────────
_DB_FILE = SQLITE_PATH

US_STATES = [
    'CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI',
    'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI',
]
CANCELLATION_REASONS = [
    'Payment Expired',
    'Vendor Split Possible',
    'Stock Sync Delay',
    'AI Logic Gap - SKU Mapping',
]


def ensure_schema(conn) -> None:
    """Create orders_feed, orders_processed, intervention_log if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS orders_feed (
            order_id            TEXT PRIMARY KEY,
            order_value         REAL,
            margin_percent      REAL,
            ai_cancel_flag      INTEGER DEFAULT 0,
            cancellation_reason TEXT,
            vendor_split_possible INTEGER DEFAULT 0,
            created_at          TEXT,
            processed_flag      INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS orders_processed (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id            TEXT,
            intervention_type   TEXT,
            intervention_success INTEGER,
            revenue_prevented   REAL,
            margin_saved        REAL,
            intervention_cost   REAL,
            net_profit_impact   REAL,
            agent_type          TEXT,
            timestamp           TEXT
        );

        CREATE TABLE IF NOT EXISTS intervention_log (
            intervention_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id            TEXT,
            action_taken        TEXT,
            agent_type          TEXT,
            intervention_time   TEXT,
            intervention_result TEXT
        );
    """)
    conn.commit()


def generate_order(counter: int) -> dict:
    """Generate a single synthetic ecommerce order."""
    qty        = random.randint(1, 5)
    price      = random.randint(20, 3500)
    value      = round(price * qty, 2)
    margin     = round(random.uniform(0.18, 0.58), 4)
    ai_cancel  = 1 if random.random() < 0.65 else 0
    reason     = random.choice(CANCELLATION_REASONS) if ai_cancel else 'N/A'
    vendor_split = 1 if reason == 'Vendor Split Possible' else 0

    return {
        'order_id':             f'FEED-{counter:08d}',
        'order_value':          value,
        'margin_percent':       margin,
        'ai_cancel_flag':       ai_cancel,
        'cancellation_reason':  reason,
        'vendor_split_possible':vendor_split,
        'created_at':           datetime.datetime.utcnow().isoformat(),
        'processed_flag':       0,
    }


def insert_orders_batch(conn, orders: list) -> None:
    """Insert a batch of orders into orders_feed (ignore duplicates)."""
    conn.executemany(
        """INSERT OR IGNORE INTO orders_feed
           (order_id, order_value, margin_percent, ai_cancel_flag,
            cancellation_reason, vendor_split_possible, created_at, processed_flag)
           VALUES
           (:order_id, :order_value, :margin_percent, :ai_cancel_flag,
            :cancellation_reason, :vendor_split_possible, :created_at, :processed_flag)
        """,
        orders,
    )
    conn.commit()


def get_feed_stats(conn) -> dict:
    """Return quick stats about the feed table."""
    total     = conn.execute("SELECT COUNT(*) FROM orders_feed").fetchone()[0]
    pending   = conn.execute("SELECT COUNT(*) FROM orders_feed WHERE processed_flag=0").fetchone()[0]
    processed = total - pending
    return {'total': total, 'pending': pending, 'processed': processed}


def run_feed(db_path: str = _DB_FILE,
             batch_size: int = 5,
             interval_seconds: float = 3.0,
             max_orders: int = 0) -> None:
    """
    Main feed loop. Inserts `batch_size` orders every `interval_seconds`.
    Runs indefinitely unless max_orders > 0.
    """
    conn = get_conn(db_path)
    ensure_schema(conn)

    # Seed counter from existing rows
    row = conn.execute("SELECT MAX(CAST(SUBSTR(order_id, 6) AS INTEGER)) FROM orders_feed").fetchone()
    counter = (row[0] or 0) + 1

    print(f"[SyntheticFeed] Starting — DB: {db_path}")
    total_inserted = 0

    try:
        while True:
            batch = [generate_order(counter + i) for i in range(batch_size)]
            insert_orders_batch(conn, batch)
            counter      += batch_size
            total_inserted += batch_size
            stats = get_feed_stats(conn)
            print(
                f"[SyntheticFeed] +{batch_size} orders | "
                f"total={stats['total']} | pending={stats['pending']} | "
                f"processed={stats['processed']}"
            )
            if max_orders > 0 and total_inserted >= max_orders:
                print(f"[SyntheticFeed] Reached max_orders={max_orders}. Stopping.")
                break
            time.sleep(interval_seconds)
    finally:
        conn.close()


if __name__ == '__main__':
    run_feed()
