# Ultra-light trade logging (append-only JSONL)
# No dependencies, no coupling to strategy logic.

import json
import uuid
from datetime import datetime
from typing import Any, Dict, Optional


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def new_trade_id() -> str:
    return str(uuid.uuid4())


def log_trade(
    data: Dict[str, Any],
    path: str = "trade_log.jsonl",
) -> None:
    """
    Append a single trade event (entry/exit) to JSONL file.
    Each line = one JSON object.
    """
    data["logged_at"] = _now_iso()

    with open(path, "a") as f:
        f.write(json.dumps(data) + "\n")


def log_entry(
    symbol: str,
    timeframe: str,
    direction: str,
    entry_price: float,
    post: Dict[str, Any],
    momentum: Dict[str, Any],
    trade_id: Optional[str] = None,
) -> str:
    """
    Log trade entry with snapshot context.
    Returns trade_id for later linking to exit.
    """
    trade_id = trade_id or new_trade_id()

    log_trade({
        "event": "entry",
        "trade_id": trade_id,
        "symbol": symbol,
        "tf": timeframe,
        "direction": direction,
        "entry_price": entry_price,

        "structure_state": post.get("structure_state"),
        "confidence": (post.get("structure_meta") or {}).get("confidence"),
        "risk_tier": (post.get("structure_meta") or {}).get("risk_tier"),
        "tags": (post.get("structure_meta") or {}).get("tags"),

        "macd_hist": momentum.get("macd_hist"),
        "macd_hist_slope_3": momentum.get("macd_hist_slope_3"),
    })

    return trade_id


def log_exit(
    trade_id: str,
    symbol: str,
    timeframe: str,
    exit_price: float,
    result_r: float,
) -> None:
    """
    Log trade exit and outcome.
    """
    log_trade({
        "event": "exit",
        "trade_id": trade_id,
        "symbol": symbol,
        "tf": timeframe,
        "exit_price": exit_price,
        "result_r": result_r,
    })
