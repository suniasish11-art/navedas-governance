"""
navedas_agent.py — Navedas Governance Agent
Polls orders_feed for unprocessed orders, applies governance rules,
writes results to orders_processed + intervention_log, marks as processed.

Run standalone:  python navedas_agent.py
Or import and call run_agent_cycle() for a single pass.
"""
import sqlite3
import time
import os
import tempfile
from datetime import datetime

from governance_engine import apply_governance_rules
from synthetic_feed_generator import ensure_schema

# ── Shared DB path ─────────────────────────────────────────────────────────────
_DB_FILE = os.path.join(tempfile.gettempdir(), 'navedas_governance.db')

POLL_INTERVAL_SECONDS = 15
BATCH_LIMIT           = 50   # max orders processed per cycle


def fetch_unprocessed(conn: sqlite3.Connection, limit: int = BATCH_LIMIT) -> list:
    """Return up to `limit` unprocessed orders from orders_feed."""
    rows = conn.execute(
        """SELECT order_id, order_value, margin_percent, ai_cancel_flag,
                  cancellation_reason, vendor_split_possible
           FROM orders_feed
           WHERE processed_flag = 0
           ORDER BY created_at ASC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    keys = ['order_id', 'order_value', 'margin_percent', 'ai_cancel_flag',
            'cancellation_reason', 'vendor_split_possible']
    return [dict(zip(keys, row)) for row in rows]


def write_processed(conn: sqlite3.Connection, result: dict) -> None:
    """Insert one processed result into orders_processed."""
    conn.execute(
        """INSERT INTO orders_processed
           (order_id, intervention_type, intervention_success,
            revenue_prevented, margin_saved, intervention_cost,
            net_profit_impact, agent_type, timestamp)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            result['order_id'],
            result['intervention_type'],
            result['intervention_success'],
            result['revenue_prevented'],
            result['margin_saved'],
            result['intervention_cost'],
            result['net_profit_impact'],
            result['agent_type'],
            result['timestamp'],
        ),
    )


def write_intervention_log(conn: sqlite3.Connection, result: dict) -> None:
    """Insert one entry into intervention_log."""
    outcome = 'SUCCESS' if result['intervention_success'] else 'FAILED'
    conn.execute(
        """INSERT INTO intervention_log
           (order_id, action_taken, agent_type, intervention_time, intervention_result)
           VALUES (?,?,?,?,?)""",
        (
            result['order_id'],
            result['intervention_type'],
            result['agent_type'],
            result['timestamp'],
            outcome,
        ),
    )


def mark_processed(conn: sqlite3.Connection, order_ids: list) -> None:
    """Set processed_flag = 1 for a list of order_ids."""
    conn.executemany(
        "UPDATE orders_feed SET processed_flag = 1 WHERE order_id = ?",
        [(oid,) for oid in order_ids],
    )


def run_agent_cycle(db_path: str = _DB_FILE) -> dict:
    """
    Single agent cycle:
    1. Fetch unprocessed orders
    2. Apply governance rules
    3. Write results
    4. Mark as processed
    Returns summary dict.
    """
    conn = sqlite3.connect(db_path)
    ensure_schema(conn)

    orders = fetch_unprocessed(conn)
    if not orders:
        conn.close()
        return {'processed': 0, 'recovered': 0, 'revenue_prevented': 0.0}

    results     = [apply_governance_rules(o) for o in orders]
    order_ids   = []
    recovered   = 0
    rev_total   = 0.0

    conn.execute('BEGIN')
    for r in results:
        write_processed(conn, r)
        write_intervention_log(conn, r)
        order_ids.append(r['order_id'])
        if r['intervention_success']:
            recovered += 1
            rev_total += r['revenue_prevented']

    mark_processed(conn, order_ids)
    conn.execute('COMMIT')
    conn.close()

    return {
        'processed':         len(results),
        'recovered':         recovered,
        'revenue_prevented': rev_total,
    }


def get_agent_summary(db_path: str = _DB_FILE) -> dict:
    """
    Return aggregate stats from orders_processed for dashboard use.
    """
    try:
        conn = sqlite3.connect(db_path)
        ensure_schema(conn)
        row = conn.execute(
            """SELECT
                COUNT(*)                          AS total,
                SUM(intervention_success)         AS recovered,
                SUM(revenue_prevented)            AS rev_prevented,
                SUM(margin_saved)                 AS margin_saved,
                SUM(intervention_cost)            AS int_cost,
                SUM(net_profit_impact)            AS net_profit
               FROM orders_processed"""
        ).fetchone()
        conn.close()
        if row and row[0]:
            return {
                'total':          int(row[0]  or 0),
                'recovered':      int(row[1]  or 0),
                'rev_prevented':  float(row[2] or 0),
                'margin_saved':   float(row[3] or 0),
                'int_cost':       float(row[4] or 0),
                'net_profit':     float(row[5] or 0),
            }
    except Exception:
        pass
    return {'total': 0, 'recovered': 0, 'rev_prevented': 0.0,
            'margin_saved': 0.0, 'int_cost': 0.0, 'net_profit': 0.0}


def get_recent_events(db_path: str = _DB_FILE, limit: int = 20) -> list:
    """
    Return recent intervention events for the Event Timeline tab.
    """
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            """SELECT il.order_id, il.action_taken, il.agent_type,
                      il.intervention_time, il.intervention_result,
                      op.revenue_prevented, op.order_value
               FROM intervention_log il
               LEFT JOIN orders_processed op ON il.order_id = op.order_id
               ORDER BY il.intervention_id DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        conn.close()
        keys = ['order_id', 'action_taken', 'agent_type', 'time',
                'result', 'revenue_prevented', 'order_value']
        return [dict(zip(keys, r)) for r in rows]
    except Exception:
        return []


def get_feed_pending_count(db_path: str = _DB_FILE) -> int:
    """Return count of unprocessed orders in the feed."""
    try:
        conn = sqlite3.connect(db_path)
        count = conn.execute(
            "SELECT COUNT(*) FROM orders_feed WHERE processed_flag = 0"
        ).fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0


def run_agent(db_path: str = _DB_FILE,
              poll_interval: float = POLL_INTERVAL_SECONDS) -> None:
    """
    Continuous agent loop. Runs forever until interrupted.
    """
    print(f"[NavedasAgent] Starting — DB: {db_path}")
    conn = sqlite3.connect(db_path)
    ensure_schema(conn)
    conn.close()

    cycle = 0
    while True:
        cycle += 1
        summary = run_agent_cycle(db_path)
        if summary['processed'] > 0:
            print(
                f"[NavedasAgent] Cycle {cycle} — "
                f"processed={summary['processed']} | "
                f"recovered={summary['recovered']} | "
                f"revenue_prevented=${summary['revenue_prevented']:,.0f}"
            )
        else:
            print(f"[NavedasAgent] Cycle {cycle} — no new orders")
        time.sleep(poll_interval)


if __name__ == '__main__':
    run_agent()
