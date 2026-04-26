# Raw trade idea storage.
# One-table storage for sanity review.
# No dedupe. No filtering. No execution.

import json
import datetime as dt
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _safe_float(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        return None if x is None else int(x)
    except Exception:
        return None


def _json(x: Any) -> str:
    return json.dumps(x, separators=(",", ":"), default=str)


def ensure_raw_trade_ideas_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_trade_ideas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id TEXT,
            symbol TEXT,
            horizon TEXT,
            tf TEXT,
            trade_type TEXT,
            strategy TEXT,
            direction TEXT,

            score INTEGER,
            confidence INTEGER,
            risk_tier TEXT,
            rr_to_tp1 REAL,

            entry_zone_json TEXT,
            stop_json TEXT,
            targets_json TEXT,
            tags_json TEXT,
            warnings_json TEXT,
            reason TEXT,

            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()


def store_raw_trade_ideas(
    conn,
    *,
    snapshot_id: Optional[str],
    trade_ideas: List[Dict[str, Any]],
) -> int:
    if not trade_ideas:
        return 0

    created_at = _now_iso()
    rows = []

    for trade in trade_ideas:
        rows.append((
            snapshot_id,
            trade.get("symbol"),
            trade.get("horizon"),
            trade.get("tf"),
            trade.get("trade_type"),
            trade.get("strategy"),
            trade.get("direction"),

            _safe_int(trade.get("score")),
            _safe_int(trade.get("confidence")),
            trade.get("risk_tier"),
            _safe_float(trade.get("rr_to_tp1")),

            _json(trade.get("entry_zone") or {}),
            _json(trade.get("stop") or {}),
            _json(trade.get("targets") or []),
            _json(trade.get("tags") or []),
            _json(trade.get("warnings") or []),
            trade.get("reason"),

            _json(trade),
            created_at,
        ))

    conn.executemany("""
        INSERT INTO raw_trade_ideas (
            snapshot_id,
            symbol,
            horizon,
            tf,
            trade_type,
            strategy,
            direction,
            score,
            confidence,
            risk_tier,
            rr_to_tp1,
            entry_zone_json,
            stop_json,
            targets_json,
            tags_json,
            warnings_json,
            reason,
            raw_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    return len(rows)
