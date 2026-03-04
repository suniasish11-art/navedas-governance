"""
feed_scheduler.py — Navedas Automated Feed Scheduler
Runs synthetic order generation every 20 seconds in a continuous loop.

Designed to run as a standalone background process alongside Streamlit.

Usage:
    python feed_scheduler.py

Environment variables (optional):
    SCHEDULER_INTERVAL=20      seconds between batches (default 20)
    SCHEDULER_BATCH_SIZE=5     orders per batch (default 5)
"""
import time
import os
import sqlite3
import tempfile
import datetime

from synthetic_feed_generator import (
    _DB_FILE, ensure_schema, generate_order,
    insert_orders_batch, get_feed_stats
)

DEFAULT_INTERVAL   = int(os.environ.get("SCHEDULER_INTERVAL",   "20"))
DEFAULT_BATCH_SIZE = int(os.environ.get("SCHEDULER_BATCH_SIZE", "5"))


def _next_counter(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT MAX(CAST(SUBSTR(order_id, 6) AS INTEGER)) FROM orders_feed"
    ).fetchone()
    return (row[0] or 0) + 1


def scheduler_tick(db_path: str = _DB_FILE,
                   batch_size: int = DEFAULT_BATCH_SIZE) -> dict:
    """
    Single scheduler tick: generate `batch_size` orders and insert them.
    Returns summary dict.
    """
    conn = sqlite3.connect(db_path)
    ensure_schema(conn)
    counter = _next_counter(conn)
    batch   = [generate_order(counter + i) for i in range(batch_size)]
    insert_orders_batch(conn, batch)
    stats = get_feed_stats(conn)
    conn.close()
    return {
        'inserted': batch_size,
        'total':    stats['total'],
        'pending':  stats['pending'],
        'processed':stats['processed'],
        'timestamp':datetime.datetime.now().strftime('%H:%M:%S'),
    }


def run_scheduler(db_path: str = _DB_FILE,
                  interval: float = DEFAULT_INTERVAL,
                  batch_size: int = DEFAULT_BATCH_SIZE) -> None:
    """
    Continuous scheduler loop.
    Inserts `batch_size` orders every `interval` seconds.
    Runs until Ctrl+C.
    """
    print("=" * 60)
    print("  Navedas Feed Scheduler")
    print(f"  Interval : {interval}s")
    print(f"  Batch    : {batch_size} orders/cycle")
    print(f"  DB       : {db_path}")
    print("  Press Ctrl+C to stop")
    print("=" * 60)

    # Ensure schema exists before starting
    conn = sqlite3.connect(db_path)
    ensure_schema(conn)
    conn.close()

    cycle = 0
    while True:
        cycle += 1
        try:
            result = scheduler_tick(db_path, batch_size)
            print(
                f"[{result['timestamp']}] Cycle {cycle:04d} | "
                f"+{result['inserted']} orders | "
                f"total={result['total']:,} | "
                f"pending={result['pending']:,} | "
                f"processed={result['processed']:,}"
            )
        except Exception as e:
            print(f"[ERROR] Cycle {cycle}: {e}")

        time.sleep(interval)


if __name__ == "__main__":
    try:
        run_scheduler()
    except KeyboardInterrupt:
        print("\n[FeedScheduler] Stopped.")
