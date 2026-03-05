"""
event_logger.py — Navedas Governance Event Logger
Logs key governance events to the event_log table.
Provides real-time event timeline for the dashboard.

Event types:
  AI_DECISION       — AI cancellation decision
  AGENT_INTERVENTION — Navedas agent acts on an order
  SIGNAL_DETECTED   — A governance signal fires
  RECOVERY          — Order successfully recovered
  REVENUE_SAVED     — Revenue prevention recorded
  CHAT_DIAGNOSTIC   — Chat assistant diagnostic triggered
  FEED_UPDATE       — New orders added to feed
  LOGIC_GAP         — AI logic gap discovered
"""
import datetime
from db import get_conn, SQLITE_PATH


EVENT_ICONS = {
    'AI_DECISION':        '🤖',
    'AGENT_INTERVENTION': '⚡',
    'SIGNAL_DETECTED':    '📡',
    'RECOVERY':           '✅',
    'REVENUE_SAVED':      '💰',
    'CHAT_DIAGNOSTIC':    '💬',
    'FEED_UPDATE':        '📦',
    'LOGIC_GAP':          '⚠️',
}

EVENT_COLORS = {
    'RECOVERY':           '#059669',
    'REVENUE_SAVED':      '#059669',
    'AGENT_INTERVENTION': '#6C63FF',
    'AI_DECISION':        '#6b7280',
    'SIGNAL_DETECTED':    '#d97706',
    'LOGIC_GAP':          '#e11d48',
    'CHAT_DIAGNOSTIC':    '#0ea5e9',
    'FEED_UPDATE':        '#374151',
}


def ensure_event_schema(conn) -> None:
    """Create event_log table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS event_log (
            event_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type      TEXT,
            order_id        TEXT,
            description     TEXT,
            impact_value    REAL DEFAULT 0,
            event_timestamp TEXT
        )
    """)
    conn.commit()


def log_event(db_path: str, event_type: str, order_id: str,
              description: str, impact_value: float = 0.0) -> None:
    """
    Log a single governance event.
    Safe to call — all errors are silently swallowed.
    """
    try:
        conn = get_conn(db_path)
        ensure_event_schema(conn)
        conn.execute(
            """INSERT INTO event_log
               (event_type, order_id, description, impact_value, event_timestamp)
               VALUES (?,?,?,?,?)""",
            (event_type, order_id or '—', description,
             float(impact_value),
             datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_event_timeline(db_path: str = SQLITE_PATH, limit: int = 40) -> list:
    """
    Fetch recent events from event_log.
    Falls back to intervention_log if event_log is empty.
    Returns list of event dicts ready for display.
    """
    try:
        conn = get_conn(db_path)
        ensure_event_schema(conn)

        # Check event_log first
        rows = conn.execute(
            """SELECT event_type, order_id, description, impact_value, event_timestamp
               FROM event_log ORDER BY event_id DESC LIMIT ?""",
            (limit,),
        ).fetchall()

        if not rows:
            # Fallback: synthesize events from intervention_log
            rows_il = conn.execute(
                """SELECT il.intervention_result, il.order_id, il.action_taken,
                          COALESCE(op.revenue_prevented, 0), il.intervention_time
                   FROM intervention_log il
                   LEFT JOIN orders_processed op ON il.order_id = op.order_id
                   ORDER BY il.intervention_id DESC LIMIT ?""",
                (limit,),
            ).fetchall()
            conn.close()
            events = []
            for result, oid, action, rev, ts in rows_il:
                etype = 'RECOVERY' if result == 'SUCCESS' else 'AGENT_INTERVENTION'
                desc = (f'Order {oid}: {action} — {"Revenue saved" if result=="SUCCESS" else "Intervention failed"}'
                        + (f' ${float(rev):,.0f}' if float(rev or 0) > 0 else ''))
                events.append(_build_event(etype, oid or '—', desc, float(rev or 0), ts or ''))
            return events

        conn.close()
        return [
            _build_event(r[0], r[1] or '—', r[2], float(r[3] or 0), r[4] or '')
            for r in rows
        ]
    except Exception:
        return []


def _build_event(event_type: str, order_id: str, description: str,
                 impact_value: float, timestamp: str) -> dict:
    return {
        'event_type':   event_type,
        'order_id':     order_id,
        'description':  description,
        'impact_value': impact_value,
        'timestamp':    timestamp[:19] if timestamp else '—',
        'icon':         EVENT_ICONS.get(event_type, '📌'),
        'color':        EVENT_COLORS.get(event_type, '#6b7280'),
    }


def log_agent_cycle_events(db_path: str, results: list) -> None:
    """
    Bulk-log events from a completed agent cycle.
    Called after run_agent_cycle() returns processed results.
    """
    for r in results:
        oid = r.get('order_id', '—')
        success = r.get('intervention_success', 0)
        rev = float(r.get('revenue_prevented', 0))
        action = r.get('intervention_type', 'Unknown')

        if success:
            log_event(db_path, 'RECOVERY', oid,
                      f'{action} succeeded — revenue saved ${rev:,.0f}', rev)
            if rev > 0:
                log_event(db_path, 'REVENUE_SAVED', oid,
                          f'Revenue prevention recorded: ${rev:,.0f}', rev)
        else:
            log_event(db_path, 'AGENT_INTERVENTION', oid,
                      f'{action} attempted — intervention failed', 0)
