import os
import json
import hashlib
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo


_BOS_FVG_LTF_STATE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_ET = ZoneInfo("America/New_York")
# This module is bridge-only: it manages BOS/FVG setup orchestration and DB trade-row lifecycle, but does not perform internal trade simulation.
DEBUG_LOGS = str(os.getenv("BOS_FVG_DEBUG_LOGS", "0")).strip().lower() in {"1", "true", "t", "yes", "y", "on"}
ENTRY_LEG_SHARES = 100
BRIDGE_VERSION = (os.getenv("BOS_FVG_LTF_VERSION") or "v1").strip() or "v1"
_RUNTIME_MODE: Dict[str, Any] = {
    "execution_enabled": True,
    "live_mode": True,
}


def set_bridge_runtime_mode(*, execution_enabled: bool, live_mode: bool) -> None:
    """
    Runtime control used by sim_worker:
      - catch-up: execution_enabled=False, live_mode=False
      - live:     execution_enabled=True,  live_mode=True
    """
    _RUNTIME_MODE["execution_enabled"] = bool(execution_enabled)
    _RUNTIME_MODE["live_mode"] = bool(live_mode)


def get_bridge_runtime_mode() -> Dict[str, Any]:
    return dict(_RUNTIME_MODE)


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    val = str(raw).strip().lower()
    if val in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if val in {"0", "false", "f", "no", "n", "off"}:
        return False
    if DEBUG_LOGS:
        print(f"[BOS-FVG-DB] env fallback used for {name}: {raw!r}")
    return default


def _safe_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    val = _safe_float(raw)
    if val is None:
        if DEBUG_LOGS:
            print(f"[BOS-FVG-DB] env fallback used for {name}: {raw!r}")
        return default
    return val


def _safe_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        if DEBUG_LOGS:
            print(f"[BOS-FVG-DB] env fallback used for {name}: {raw!r}")
        return default


def _parse_ts(ts: Any) -> Optional[datetime]:
    if isinstance(ts, datetime):
        return ts
    if not isinstance(ts, str) or not ts:
        return None
    t = ts.strip()
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(t)
    except ValueError:
        return None


def _ts_to_iso(ts: Optional[datetime]) -> Optional[str]:
    return ts.isoformat() if ts else None


def _as_et_str(ts_value: Any) -> Optional[str]:
    dt = _parse_ts(ts_value)
    if dt is None:
        if isinstance(ts_value, str) and ts_value:
            return ts_value
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(_ET).isoformat()


def _end_time_et_for_day(ts_value: Any) -> Optional[str]:
    dt = _parse_ts(ts_value)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    et = dt.astimezone(_ET)
    return et.replace(hour=15, minute=58, second=0, microsecond=0, tzinfo=None).isoformat(sep=" ")

def _latest_swing(swings: Dict[str, Any], swing_type: str, current_ts: Optional[datetime]) -> Optional[Dict[str, Any]]:
    swing_items = (swings or {}).get("swings") or []
    latest = None
    latest_dt = None
    for s in swing_items:
        if s.get("type") != swing_type:
            continue
        s_dt = _parse_ts(s.get("ts"))
        if current_ts and s_dt and s_dt >= current_ts:
            continue
        if latest is None:
            latest = s
            latest_dt = s_dt
            continue
        if s_dt and (latest_dt is None or s_dt > latest_dt):
            latest = s
            latest_dt = s_dt
        elif latest_dt is None and str(s.get("ts") or "") > str((latest or {}).get("ts") or ""):
            latest = s
    return latest


def _default_cfg() -> Dict[str, Any]:
    return {
        "enabled": _safe_bool_env("BOS_SCORE_ENABLED", True),
        "score_min": _safe_float_env("BOS_SCORE_MIN", 50.0),
        "weight_momentum": _safe_float_env("BOS_WEIGHT_MOMENTUM", 25.0),
        "weight_volume": _safe_float_env("BOS_WEIGHT_VOLUME", 25.0),
        "weight_close": _safe_float_env("BOS_WEIGHT_CLOSE", 25.0),
        "weight_break": _safe_float_env("BOS_WEIGHT_BREAK", 25.0),
        "mom_threshold": _safe_float_env("BOS_MOM_THRESHOLD", 0.8),
        "vol_threshold": _safe_float_env("BOS_VOL_THRESHOLD", 2.0),
        "close_threshold": _safe_float_env("BOS_CLOSE_THRESHOLD", 0.7),
        "break_threshold": _safe_float_env("BOS_BREAK_THRESHOLD", 0.001),
        "initial_capital": _safe_float_env("BOS_INITIAL_CAPITAL", 300000.0),
        "shares_per_trade": _safe_int_env("BOS_SHARES_PER_TRADE", 100),
        "max_open_positions": _safe_int_env("BOS_MAX_OPEN_POSITIONS", 1),
        "trade_score_min": _safe_float_env("BOS_FVG_LTF_TRADE_SCORE_MIN", 0.0),
    }


def _ensure_state(symbol: str, timeframe: str) -> Dict[str, Any]:
    key = (symbol, timeframe)
    if key not in _BOS_FVG_LTF_STATE:
        cfg = _default_cfg()
        _BOS_FVG_LTF_STATE[key] = {
            "config": cfg,
            "pending_setup": None,
            "pending_rearm_setup": None,
            "broken_swing_highs": set(),
            "broken_swing_lows": set(),
            "trade_id_counter": 0,
            "signal_id_counter": 0,
            "signals": [],
            "latest_snapshot": None,
            "bridge_rows": [],
            "bridge_setup_index": {},
            "bridge_current_setup_id": None,
            "bridge_last_swing_ts": {},
            "bridge_avg_entry": {},
            "bridge_avg_qty": {},
            "bridge_eod_close_sent": {},
        }
    return _BOS_FVG_LTF_STATE[key]


def _deterministic_setup_id(
    symbol: str,
    timeframe: str,
    bos_ts: Any,
    version: str,
    side: str,
    fvg_high: Optional[float],
    fvg_low: Optional[float],
) -> str:
    bos_ts_s = str(bos_ts or "")
    stable = "|".join([
        symbol,
        timeframe,
        bos_ts_s,
        version,
        str(side or ""),
        f"{_safe_float(fvg_high, 0.0):.8f}",
        f"{_safe_float(fvg_low, 0.0):.8f}",
    ])
    h = hashlib.sha1(stable.encode("utf-8")).hexdigest()[:10]
    return f"{symbol}_{timeframe}_{bos_ts_s}_{version}_{h}"


def _bridge_trade_log(action: str, row: Dict[str, Any], reason: str) -> None:
    tags = row.get("tags") or []
    setup_id = row.get("setup_id")
    if not setup_id:
        for tag in tags:
            if isinstance(tag, str) and tag.startswith("id:"):
                setup_id = tag.split(":", 1)[1]
                break
    print(
        f"[BOS-FVG-DB][TRADE_STATE_LOG] ACTION={action} | Symbol={row.get('symbol')} | TF={row.get('entry_tf')} | "
        f"ID={setup_id} | Leg={row.get('leg')} | Trade={row.get('trade')} | Entry={row.get('entry_level')} | "
        f"SL={row.get('sl_level')} | Status={row.get('status')} | Manage={row.get('manage')} | Reason={reason}"
    )


def _bridge_insert_rows(
    state: Dict[str, Any],
    symbol: str,
    timeframe: str,
    side: str,
    version: str,
    strategy_name: str,
    bos_ts: Any,
    fvg_high: Optional[float],
    fvg_low: Optional[float],
) -> Optional[str]:
    setup_id = _deterministic_setup_id(symbol, timeframe, bos_ts, version, side, fvg_high, fvg_low)
    if setup_id in state["bridge_setup_index"]:
        return None
    cp = "call" if side == "long" else "put"
    sl_cond = "cb" if side == "long" else "ca"
    sl_level = fvg_low if side == "long" else fvg_high
    end_time_et = _end_time_et_for_day(bos_ts)
    fvg_mid = None
    if fvg_high is not None and fvg_low is not None:
        fvg_mid = (_safe_float(fvg_high, 0.0) + _safe_float(fvg_low, 0.0)) / 2.0

    # 3-leg ladder:
    # longs  -> high(1), mid(2), low(3)
    # shorts -> low(1), mid(2), high(3)
    if side == "long":
        leg_levels = [
            (1, _safe_float(fvg_high), 1),
            (2, _safe_float(fvg_mid), 2),
            (3, _safe_float(fvg_low), 3),
        ]
    else:
        leg_levels = [
            (1, _safe_float(fvg_low), 1),
            (2, _safe_float(fvg_mid), 2),
            (3, _safe_float(fvg_high), 3),
        ]
    rows: List[Dict[str, Any]] = []
    for leg, entry_level, qty_per_trade in leg_levels:
        for trade in (1, 2):
            tags = [
                f"strategy:{strategy_name}",
                f"version:{version}",
                f"id:{setup_id}",
                f"tf:{timeframe}",
                f"leg:{leg}",
                f"trade:{trade}",
            ]
            row = {
                "setup_id": setup_id,
                "symbol": symbol,
                "asset_type": "option",
                "cp": cp,
                "qty": qty_per_trade,
                "entry_type": "equity",
                "entry_cond": "at",
                "entry_level": entry_level,
                "entry_tf": timeframe,
                "sl_type": "equity",
                "sl_cond": sl_cond,
                "sl_level": sl_level,
                "sl_tf": timeframe,
                "tp_type": None,
                "tp_level": None,
                "status": "nt-waiting",
                "manage": None,
                "db_new_insert_confirmed": False,
                "db_active_seen": False,
                "db_active_status": None,
                "db_active_manage": None,
                "end_time_et": end_time_et,
                "leg": leg,
                "trade": trade,
                "tags": tags,
            }
            rows.append(row)
            _bridge_trade_log("INSERT", row, "stage1_create")
    state["bridge_rows"].extend(rows)
    state["bridge_setup_index"][setup_id] = {
        "rows": rows,
        "side": side,
        "bridge_live_phase_entered": False,
    }
    state["bridge_current_setup_id"] = setup_id
    return setup_id


def _calc_avg_entry_from_rows(rows: List[Dict[str, Any]]) -> Tuple[Optional[float], float]:
    total_qty = 0.0
    total_notional = 0.0
    for row in rows or []:
        qty = _safe_float(row.get("qty"), 0.0) or 0.0
        entry_level = _safe_float(row.get("entry_level"))
        if qty <= 0 or entry_level is None:
            continue
        total_qty += qty
        total_notional += entry_level * qty
    if total_qty <= 0:
        return None, 0.0
    return total_notional / total_qty, total_qty


def _is_eod_close_time(last_ts_value: Any) -> bool:
    dt_obj = _parse_ts(last_ts_value)
    if dt_obj is None:
        return False
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=ZoneInfo("UTC"))
    et = dt_obj.astimezone(_ET)
    return (et.hour > 15) or (et.hour == 15 and et.minute >= 56)


def _bridge_setup_rows(state: Dict[str, Any], setup_id: Optional[str]) -> List[Dict[str, Any]]:
    if not setup_id:
        return []
    return [r for r in state["bridge_rows"] if r.get("setup_id") == setup_id]


def _clear_pending_setup_for_new_bos(
    state: Dict[str, Any],
    *,
    symbol: str,
    timeframe: str,
    reason: str,
    new_side: Optional[str] = None,
    new_bos_ts: Optional[str] = None,
) -> None:
    pending = state.get("pending_setup")
    if not pending:
        return
    if DEBUG_LOGS:
        _db_state_log(
            symbol,
            timeframe,
            "pending_setup_invalidated",
            reason=reason,
            old_trade_id=pending.get("trade_id"),
            old_setup_id=pending.get("setup_id"),
            old_side=pending.get("side"),
            old_bos_ts=pending.get("bos_ts"),
            new_side=new_side,
            new_bos_ts=new_bos_ts,
        )
    state["pending_setup"] = None
    if state.get("bridge_current_setup_id") == pending.get("setup_id"):
        state["bridge_current_setup_id"] = None


def _bridge_tag_value(row: Dict[str, Any], prefix: str) -> Optional[str]:
    tags = row.get("tags") or []
    want = f"{prefix}:"
    for tag in tags:
        if isinstance(tag, str) and tag.startswith(want):
            return tag.split(":", 1)[1].strip() or None
    return None


def _bridge_scope_rows(
    state: Dict[str, Any],
    *,
    symbol: str,
    timeframe: str,
    strategy_name: str = "bos_fvg_ltf",
    version: str = BRIDGE_VERSION,
) -> List[Dict[str, Any]]:
    scoped: List[Dict[str, Any]] = []
    for row in state.get("bridge_rows", []) or []:
        if str(row.get("symbol") or "") != str(symbol):
            continue
        if str(row.get("entry_tf") or "") != str(timeframe):
            continue
        if _bridge_tag_value(row, "strategy") != strategy_name:
            continue
        if _bridge_tag_value(row, "version") != version:
            continue
        scoped.append(row)
    return scoped


def _bridge_update_rows(rows: List[Dict[str, Any]], updates: Dict[str, Any], reason: str) -> int:
    changed = 0
    for row in rows:
        row_changed = False
        for key, value in updates.items():
            if row.get(key) != value:
                row[key] = value
                row_changed = True
        if row_changed:
            changed += 1
            _bridge_trade_log("UPDATE", row, reason)
    return changed


def _log_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    return str(value)


def _db_state_log(symbol: str, timeframe: str, stage: str, **fields: Any) -> None:
    parts = [f"[BOS-FVG-DB][STATE] Stage={stage}", f"Symbol={symbol}", f"TF={timeframe}"]
    for k, v in fields.items():
        parts.append(f"{k}={_log_value(v)}")
    print(" | ".join(parts))


def _pending_summary(pending: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(pending, dict):
        return {
            "pending_exists": False,
            "trade_id": None,
            "side": None,
            "bos_ts": None,
            "setup_id": None,
            "has_fvg": False,
            "cancel_requested": False,
        }
    return {
        "pending_exists": True,
        "trade_id": pending.get("trade_id"),
        "side": pending.get("side"),
        "bos_ts": pending.get("bos_ts"),
        "setup_id": pending.get("setup_id"),
        "has_fvg": bool(pending.get("fvg")),
        "cancel_requested": bool(pending.get("cancel_requested")),
    }


def _rearm_summary(rearm: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(rearm, dict):
        return {
            "rearm_exists": False,
            "rearm_side": None,
            "rearm_bos_ts": None,
        }
    return {
        "rearm_exists": True,
        "rearm_side": rearm.get("side"),
        "rearm_bos_ts": rearm.get("bos_ts"),
    }


def _bridge_counts(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "scope_rows": len(rows),
        "seen_rows": sum(1 for r in rows if r.get("db_active_seen")),
        "waiting_rows": sum(1 for r in rows if r.get("db_active_status") == "nt-waiting"),
        "managing_rows": sum(1 for r in rows if r.get("db_active_status") == "nt-managing"),
        "manage_c_rows": sum(1 for r in rows if str(r.get("manage") or "") == "C"),
        "db_manage_c_rows": sum(1 for r in rows if str(r.get("db_active_manage") or "") == "C"),
        "new_insert_confirmed_rows": sum(1 for r in rows if bool(r.get("db_new_insert_confirmed"))),
    }


def get_live_bridge_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for state in _BOS_FVG_LTF_STATE.values():
        rows.extend(deepcopy(state.get("bridge_rows") or []))
    return rows


def apply_live_bridge_db_state(updates: List[Dict[str, Any]]) -> None:
    if not updates:
        return
    keyed: Dict[Tuple[str, int, int], Dict[str, Any]] = {}
    for u in updates:
        setup_id = str(u.get("setup_id") or "").strip()
        leg = _safe_int(u.get("leg"), 0)
        trade = _safe_int(u.get("trade"), 0)
        if not setup_id or leg <= 0 or trade <= 0:
            continue
        keyed[(setup_id, leg, trade)] = u
    if not keyed:
        return
    for state in _BOS_FVG_LTF_STATE.values():
        for row in state.get("bridge_rows") or []:
            key = (str(row.get("setup_id") or "").strip(), _safe_int(row.get("leg"), 0), _safe_int(row.get("trade"), 0))
            incoming = keyed.get(key)
            if not incoming:
                continue
            row["db_new_insert_confirmed"] = bool(incoming.get("db_new_insert_confirmed", False))
            row["db_active_seen"] = bool(incoming.get("db_active_seen", False))
            row["db_active_status"] = incoming.get("db_active_status")
            row["db_active_manage"] = incoming.get("db_active_manage")


def _normalize_fvg_direction(v: Any) -> str:
    s = str(v or "").lower()
    if s in {"bull", "bullish", "long", "up"}:
        return "bull"
    if s in {"bear", "bearish", "short", "down"}:
        return "bear"
    return ""


def _extract_fvg_score_fields(fvg: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], str, str]:
    style = fvg.get("style") if isinstance(fvg.get("style"), dict) else {}

    # IMPORTANT: do not use generic `score` or style.confidence as FVG/trade quality
    # in this strategy. Those are often normalized event-style scores and can mask
    # the true per-FVG values.
    trade_score = _safe_float(fvg.get("trade_score"))
    trade_src = "trade_score"
    if trade_score is None:
        trade_score = _safe_float(style.get("trade_score"))
        trade_src = "style.trade_score"

    fvg_score = _safe_float(fvg.get("fvg_score"))
    fvg_src = "fvg_score"
    if fvg_score is None:
        fvg_score = _safe_float(style.get("fvg_score"))
        fvg_src = "style.fvg_score"

    if fvg_score is None and trade_score is not None:
        fvg_score = trade_score
        fvg_src = f"fallback:{trade_src}"
    if trade_score is None and fvg_score is not None:
        trade_score = fvg_score
        trade_src = f"fallback:{fvg_src}"

    if fvg_score is None:
        fvg_src = "missing"
    if trade_score is None:
        trade_src = "missing"

    return fvg_score, trade_score, fvg_src, trade_src


def _select_first_post_bos_fvg(fvgs: List[Dict[str, Any]], setup_side: str, bos_dt: Optional[datetime]) -> Optional[Dict[str, Any]]:
    if not bos_dt:
        return None
    want = "bull" if setup_side == "long" else "bear"
    matches: List[Tuple[datetime, Dict[str, Any]]] = []
    for f in fvgs or []:
        if _normalize_fvg_direction(f.get("direction")) != want:
            continue
        if bool(f.get("filled", False)):
            continue
        cdt = _parse_ts(f.get("created_ts"))
        if cdt is None or cdt <= bos_dt:
            continue
        matches.append((cdt, f))
    if not matches:
        return None
    matches.sort(key=lambda x: x[0])
    return matches[0][1]


def _match_selected_fvg(
    selected_fvg: Optional[Dict[str, Any]],
    fvg_source: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not isinstance(selected_fvg, dict):
        return None
    created_ts = selected_fvg.get("created_ts")
    direction = _normalize_fvg_direction(selected_fvg.get("direction"))
    if not created_ts or not direction:
        return None
    for f in fvg_source or []:
        if _normalize_fvg_direction(f.get("direction")) != direction:
            continue
        if f.get("created_ts") == created_ts:
            return f
    return None


def _refresh_selected_fvg_fields(
    selected_fvg: Optional[Dict[str, Any]],
    fvg_source: List[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if not isinstance(selected_fvg, dict):
        return selected_fvg, None
    matched = _match_selected_fvg(selected_fvg, fvg_source or [])
    if not matched:
        return selected_fvg, None
    fvg_score, trade_score, fvg_score_src, trade_score_src = _extract_fvg_score_fields(matched)
    selected_fvg["direction"] = matched.get("direction")
    selected_fvg["low"] = _safe_float(matched.get("low"))
    selected_fvg["high"] = _safe_float(matched.get("high"))
    selected_fvg["filled"] = bool(matched.get("filled", False))
    selected_fvg["filled_ts"] = matched.get("filled_ts")
    selected_fvg["fvg_score"] = fvg_score
    selected_fvg["trade_score"] = trade_score
    selected_fvg["fvg_score_src"] = fvg_score_src
    selected_fvg["trade_score_src"] = trade_score_src
    return selected_fvg, matched

def evaluate_bos_fvg_ltf(
    symbol: str,
    timeframe: str,
    candles: List[Dict[str, Any]],
    swings: Dict[str, Any],
    structure_state_tf: Optional[str] = None,
    structure_state_15m: Optional[str] = None,
    structure_state_1h: Optional[str] = None,
    fvgs: Optional[List[Dict[str, Any]]] = None,
    spot_last_candle: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    state = _ensure_state(symbol, timeframe)
    cfg = state["config"]
    runtime = get_bridge_runtime_mode()
    execution_enabled = bool(runtime.get("execution_enabled", True))
    live_mode = bool(runtime.get("live_mode", True))

    if str(timeframe or "").lower() not in {"1m", "3m", "5m"}:
        return {
            "id": "bos_fvg_ltf",
            "symbol": symbol,
            "timeframe": timeframe,
            "status": "skipped",
            "skip_reason": "ltf_only_timeframe_filter",
            "pending_setup_side": (state.get("pending_setup") or {}).get("side"),
            "pending_setup": deepcopy(state.get("pending_setup")),
            "signal_count": len(state["signals"]),
            "last_signal": deepcopy(state["signals"][-1] if state["signals"] else None),
            "bridge_current_setup_id": state.get("bridge_current_setup_id"),
            "live_trade_rows": deepcopy(state.get("bridge_rows") or []),
            "execution_enabled": execution_enabled,
            "live_mode": live_mode,
        }

    last_candle = candles[-1] if candles else {}
    last_ts_raw = last_candle.get("ts") or last_candle.get("timestamp")
    last_dt = _parse_ts(last_ts_raw)
    last_ts = _ts_to_iso(last_dt) or (str(last_ts_raw) if last_ts_raw is not None else None)
    last_ts_et = last_candle.get("ts_et") or _as_et_str(last_ts)

    high_px = _safe_float(last_candle.get("high"))
    low_px = _safe_float(last_candle.get("low"))
    close_px = _safe_float(last_candle.get("close"))

    recent_high = _latest_swing(swings, "swing_high", last_dt)
    recent_low = _latest_swing(swings, "swing_low", last_dt)
    recent_high_price = _safe_float((recent_high or {}).get("price"))
    recent_low_price = _safe_float((recent_low or {}).get("price"))
    recent_high_ts = (recent_high or {}).get("ts")
    recent_low_ts = (recent_low or {}).get("ts")
    recent_high_pivot_score = _safe_float(((recent_high or {}).get("pivot") or {}).get("score"))
    recent_low_pivot_score = _safe_float(((recent_low or {}).get("pivot") or {}).get("score"))

    mom_val = _safe_float(last_candle.get("mom_atr"), 0.0) or 0.0
    vol_val = _safe_float(last_candle.get("vol_rel"), 0.0) or 0.0

    candle_range = (high_px - low_px) if (high_px is not None and low_px is not None) else None
    close_strength_long = ((close_px - low_px) / candle_range) if (candle_range and close_px is not None and low_px is not None) else 0.0
    close_strength_short = ((high_px - close_px) / candle_range) if (candle_range and close_px is not None and high_px is not None) else 0.0

    break_distance_long = ((close_px - recent_high_price) / recent_high_price) if (close_px is not None and recent_high_price and recent_high_price > 0) else 0.0
    break_distance_short = ((recent_low_price - close_px) / recent_low_price) if (close_px is not None and recent_low_price and recent_low_price > 0) else 0.0

    momentum_pass_long = mom_val >= cfg["mom_threshold"]
    volume_pass_long = vol_val >= cfg["vol_threshold"]
    close_pass_long = bool(candle_range and close_strength_long >= cfg["close_threshold"])
    break_pass_long = bool(recent_high_price is not None and break_distance_long >= cfg["break_threshold"])

    momentum_pass_short = mom_val >= cfg["mom_threshold"]
    volume_pass_short = vol_val >= cfg["vol_threshold"]
    close_pass_short = bool(candle_range and close_strength_short >= cfg["close_threshold"])
    break_pass_short = bool(recent_low_price is not None and break_distance_short >= cfg["break_threshold"])

    score_total_long = (cfg["weight_momentum"] if momentum_pass_long else 0.0) + (cfg["weight_volume"] if volume_pass_long else 0.0) + (cfg["weight_close"] if close_pass_long else 0.0) + (cfg["weight_break"] if break_pass_long else 0.0)
    score_total_short = (cfg["weight_momentum"] if momentum_pass_short else 0.0) + (cfg["weight_volume"] if volume_pass_short else 0.0) + (cfg["weight_close"] if close_pass_short else 0.0) + (cfg["weight_break"] if break_pass_short else 0.0)
    score_pass_long = score_total_long >= cfg["score_min"]
    score_pass_short = score_total_short >= cfg["score_min"]

    long_bos_detected = False
    short_bos_detected = False
    long_already_broken = False
    short_already_broken = False
    swing_high_key = None
    swing_low_key = None

    if recent_high_price is not None and recent_high_ts is not None:
        swing_high_key = f"{recent_high_ts}|{recent_high_price}"
        long_already_broken = swing_high_key in state["broken_swing_highs"]
        if not long_already_broken and close_px is not None and close_px > recent_high_price:
            long_bos_detected = True
    if recent_low_price is not None and recent_low_ts is not None:
        swing_low_key = f"{recent_low_ts}|{recent_low_price}"
        short_already_broken = swing_low_key in state["broken_swing_lows"]
        if not short_already_broken and close_px is not None and close_px < recent_low_price:
            short_bos_detected = True

    if long_bos_detected and swing_high_key:
        state["broken_swing_highs"].add(swing_high_key)
        print(f"[BOS-FVG-DB] BOS detected LONG | Symbol={symbol} | TF={timeframe} | Break={recent_high_price}")
    if short_bos_detected and swing_low_key:
        state["broken_swing_lows"].add(swing_low_key)
        print(f"[BOS-FVG-DB] BOS detected SHORT | Symbol={symbol} | TF={timeframe} | Break={recent_low_price}")

    chosen_side = "none"
    chosen_bos_detected = False
    chosen_score_total = 0.0
    chosen_score_pass = False
    chosen_momentum_pass = False
    chosen_volume_pass = False
    chosen_close_pass = False
    chosen_break_pass = False
    chosen_close_strength = 0.0
    chosen_break_distance = 0.0

    if long_bos_detected and short_bos_detected:
        chosen_side = "long" if score_total_long >= score_total_short else "short"
    elif long_bos_detected:
        chosen_side = "long"
    elif short_bos_detected:
        chosen_side = "short"

    if chosen_side == "long":
        chosen_bos_detected = True
        chosen_score_total = score_total_long
        chosen_score_pass = score_pass_long
        chosen_momentum_pass = momentum_pass_long
        chosen_volume_pass = volume_pass_long
        chosen_close_pass = close_pass_long
        chosen_break_pass = break_pass_long
        chosen_close_strength = close_strength_long
        chosen_break_distance = break_distance_long
    elif chosen_side == "short":
        chosen_bos_detected = True
        chosen_score_total = score_total_short
        chosen_score_pass = score_pass_short
        chosen_momentum_pass = momentum_pass_short
        chosen_volume_pass = volume_pass_short
        chosen_close_pass = close_pass_short
        chosen_break_pass = break_pass_short
        chosen_close_strength = close_strength_short
        chosen_break_distance = break_distance_short

    status = "idle"
    skip_reason = ""

    if DEBUG_LOGS:
        pending_dbg = _pending_summary(state.get("pending_setup"))
        rearm_dbg = _rearm_summary(state.get("pending_rearm_setup"))
        _db_state_log(
            symbol,
            timeframe,
            "eval_start",
            last_ts=last_ts,
            chosen_side=chosen_side,
            chosen_bos_detected=chosen_bos_detected,
            chosen_score_pass=chosen_score_pass,
            execution_enabled=execution_enabled,
            live_mode=live_mode,
            **pending_dbg,
            **rearm_dbg,
        )

    # ------------------------------------------------------------
    # Catch-up invalidation rules
    # 1) If selected FVG becomes filled in catch-up, invalidate setup
    #    and go back to waiting for a new BOS.
    # 2) If a new BOS occurs while a setup is pending in catch-up,
    #    invalidate the old setup immediately and track the new BOS.
    # ------------------------------------------------------------
    pending = state.get("pending_setup")
    if pending and (not execution_enabled or not live_mode):
        selected_fvg = pending.get("fvg")
        if isinstance(selected_fvg, dict):
            selected_fvg, _matched_fvg = _refresh_selected_fvg_fields(selected_fvg, fvgs or [])
            pending["fvg"] = selected_fvg
            if DEBUG_LOGS:
                _db_state_log(
                    symbol,
                    timeframe,
                    "fvg_refresh_existing",
                    trade_id=pending.get("trade_id"),
                    setup_id=pending.get("setup_id"),
                    fvg_ts=selected_fvg.get("created_ts"),
                    fvg_filled=bool(selected_fvg.get("filled")),
                    trade_score=selected_fvg.get("trade_score"),
                )
            if bool(selected_fvg.get("filled")):
                _clear_pending_setup_for_new_bos(
                    state,
                    symbol=symbol,
                    timeframe=timeframe,
                    reason="catchup_fvg_filled",
                )
                state["pending_rearm_setup"] = None
                pending = None

    pending = state.get("pending_setup")

    # ------------------------------------------------------------
    # Live-boundary materialization:
    # If catch-up already formed a pending setup + selected FVG, the
    # first live candle is allowed to materialize Phase 1 by creating
    # bridge rows at that point.
    # ------------------------------------------------------------
    pending = state.get("pending_setup")
    if (
        pending
        and execution_enabled
        and live_mode
        and pending.get("fvg")
        and not pending.get("setup_id")
    ):
        fvg_now = pending.get("fvg") or {}
        setup_id = _bridge_insert_rows(
            state=state,
            symbol=symbol,
            timeframe=timeframe,
            side=pending.get("side"),
            version=BRIDGE_VERSION,
            strategy_name="bos_fvg_ltf",
            bos_ts=pending.get("bos_ts"),
            fvg_high=_safe_float(fvg_now.get("high")),
            fvg_low=_safe_float(fvg_now.get("low")),
        )
        pending["setup_id"] = setup_id or _deterministic_setup_id(
            symbol,
            timeframe,
            pending.get("bos_ts"),
            BRIDGE_VERSION,
            pending.get("side"),
            _safe_float(fvg_now.get("high")),
            _safe_float(fvg_now.get("low")),
        )
        pending["notes"] = "live_materialized_from_existing_pending_setup"
        if DEBUG_LOGS:
            _db_state_log(
                symbol,
                timeframe,
                "live_boundary_materialize_pending_setup",
                trade_id=pending.get("trade_id"),
                setup_id=pending.get("setup_id"),
                fvg_ts=fvg_now.get("created_ts"),
            )

    # ------------------------------------------------------------
    # Phase 2 -> Phase 3 transition handling (runs every candle)
    # ------------------------------------------------------------
    # Once ANY matching active row is nt-managing, this setup is LIVE.
    # At that point it must no longer remain as a pending/armed setup.
    pending = state.get("pending_setup")
    if pending and pending.get("setup_id"):
        pending_setup_rows = _bridge_setup_rows(state, pending.get("setup_id"))
        pending_live_rows = [r for r in pending_setup_rows if r.get("db_active_status") == "nt-managing"]
        if pending_live_rows:
            if DEBUG_LOGS:
                _db_state_log(
                    symbol,
                    timeframe,
                    "phase2_to_phase3_live_detected",
                    trade_id=pending.get("trade_id"),
                    setup_id=pending.get("setup_id"),
                    managing_rows=len(pending_live_rows),
                    waiting_rows=sum(1 for r in pending_setup_rows if r.get("db_active_status") == "nt-waiting"),
                )
            state["pending_setup"] = None
            state["pending_rearm_setup"] = None
            setup_meta = (state.get("bridge_setup_index") or {}).get(pending.get("setup_id")) or {}
            setup_meta["bridge_live_phase_entered"] = True
            pending = None
            status = "live"
            skip_reason = ""

    # ------------------------------------------------------------
    # Phase 2 cancel-confirmation handling (runs every candle)
    # ------------------------------------------------------------
    # If Phase 2 case 2 already requested manage="C" on the current setup,
    # do NOT wait for rows to be removed. Only wait for DB-fed confirmation
    # that the waiting rows for this setup have db_active_manage == "C".
    # Once confirmed, clear the old setup so the orchestrator can move back
    # to Phase 1 and allow the rearm candidate to become the new pending setup.
    pending = state.get("pending_setup")
    if pending and bool(pending.get("cancel_requested")) and pending.get("setup_id"):
        setup_rows = _bridge_setup_rows(state, pending.get("setup_id"))
        local_manage_c_rows = [r for r in setup_rows if str(r.get("manage") or "") == "C"]
        local_waiting_c_rows = [
            r for r in local_manage_c_rows
            if str(r.get("db_active_status") or "") in {"", "nt-waiting", "null", "None"}
        ]
        local_waiting_keys = {
            (_safe_int(r.get("leg"), 0), _safe_int(r.get("trade"), 0))
            for r in local_waiting_c_rows
        }
        # Only rows that were actually seen in DB matter for confirmation.
        seen_rows = [r for r in setup_rows if r.get("db_active_seen")]
        waiting_rows = [r for r in seen_rows if r.get("db_active_status") == "nt-waiting"]
        seen_waiting_keys = {
            (_safe_int(r.get("leg"), 0), _safe_int(r.get("trade"), 0))
            for r in waiting_rows
        }
        missing_waiting_keys = local_waiting_keys - seen_waiting_keys
        if DEBUG_LOGS:
            _db_state_log(
                symbol,
                timeframe,
                "phase2_cancel_check",
                trade_id=pending.get("trade_id"),
                setup_id=pending.get("setup_id"),
                cancel_requested=bool(pending.get("cancel_requested")),
                local_manage_c_rows=len(local_manage_c_rows),
                local_waiting_c_rows=len(local_waiting_c_rows),
                setup_rows=len(setup_rows),
                seen_rows=len(seen_rows),
                waiting_rows=len(waiting_rows),
                db_manage_c_waiting=sum(
                    1 for r in waiting_rows if str(r.get("db_active_manage") or "") == "C"
                ),
                missing_waiting_rows_after_cancel=len(missing_waiting_keys),
                bridge_current_setup_id=state.get("bridge_current_setup_id"),
            )

        cancel_confirmed = False

        # Path 1: DB still shows waiting rows and all of them reflect manage=C.
        if seen_rows and waiting_rows and all(
            str(r.get("db_active_manage") or "") == "C"
            for r in waiting_rows
        ):
            cancel_confirmed = True

        # Path 2: we requested local manage=C, and DB no longer shows any waiting
        # rows for the rows we attempted to cancel. This includes the case where
        # those rows were removed immediately by the trading manager.
        if local_waiting_c_rows and not waiting_rows:
            cancel_confirmed = True

        # Path 3: partial disappearance is also enough. If some or all of the
        # waiting rows that were marked C locally are no longer surfaced as
        # waiting rows in DB, treat that as confirmation.
        if local_waiting_c_rows and missing_waiting_keys:
            cancel_confirmed = True

        # Path 4: if the setup no longer has any surfaced active state after we
        # requested cancel, also treat that as confirmed enough to rearm.
        if local_manage_c_rows and not seen_rows:
            cancel_confirmed = True

        if cancel_confirmed:
            if DEBUG_LOGS:
                print(
                    f"[BOS-FVG-DB] phase2 cancel confirmed | Symbol={symbol} | TF={timeframe} | "
                    f"TradeID={pending.get('trade_id')} | SetupID={pending.get('setup_id')}"
                )
            state["pending_setup"] = None
            state["pending_rearm_setup"] = state.get("pending_rearm_setup")
            # Release current bridge pointer so the next setup can become current.
            if state.get("bridge_current_setup_id") == pending.get("setup_id"):
                state["bridge_current_setup_id"] = None
            setup_meta = (state.get("bridge_setup_index") or {}).get(pending.get("setup_id")) or {}
            setup_meta["bridge_live_phase_entered"] = False
            pending = None
            if DEBUG_LOGS:
                _db_state_log(
                    symbol,
                    timeframe,
                    "phase2_to_phase1_rearm_ready",
                    source="cancel_confirmed",
                )

    # Setup state management
    if cfg["enabled"] and chosen_bos_detected and chosen_score_pass and cfg["max_open_positions"] >= 1:
        pending = state["pending_setup"]
        if pending:
            # In catch-up, a new BOS invalidates the current pending setup
            # immediately. Then we proceed to publish the new BOS as the new
            # pending setup below.
            if not execution_enabled or not live_mode:
                _clear_pending_setup_for_new_bos(
                    state,
                    symbol=symbol,
                    timeframe=timeframe,
                    reason="catchup_new_bos_replaces_pending",
                    new_side=chosen_side,
                    new_bos_ts=last_ts,
                )
                state["pending_rearm_setup"] = None
                pending = None

        if pending:
            scope_rows = _bridge_scope_rows(
                state,
                symbol=symbol,
                timeframe=timeframe,
                strategy_name="bos_fvg_ltf",
                version=BRIDGE_VERSION,
            )
            if DEBUG_LOGS:
                scope_dbg = _bridge_counts(scope_rows)
                _db_state_log(
                    symbol,
                    timeframe,
                    "bos_with_existing_pending",
                    trade_id=pending.get("trade_id"),
                    setup_id=pending.get("setup_id"),
                    pending_side=pending.get("side"),
                    chosen_side=chosen_side,
                    has_live_rows=any(r.get("db_active_status") == "nt-managing" for r in scope_rows),
                    **scope_dbg,
                    **_rearm_summary(state.get("pending_rearm_setup")),
                )
            has_live_rows = any(r.get("db_active_status") == "nt-managing" for r in scope_rows)
            if not has_live_rows:
                state["pending_rearm_setup"] = {
                    "side": chosen_side,
                    "bos_ts": last_ts,
                    "bos_ts_et": last_ts_et,
                    "entry_ref_swing_high": recent_high_price,
                    "entry_ref_swing_high_ts": recent_high_ts,
                    "entry_ref_swing_high_score": recent_high_pivot_score,
                    "entry_ref_swing_low": recent_low_price,
                    "entry_ref_swing_low_ts": recent_low_ts,
                    "entry_ref_swing_low_score": recent_low_pivot_score,
                    "score_total": chosen_score_total,
                    "score_pass": chosen_score_pass,
                    "momentum_pass": chosen_momentum_pass,
                    "volume_pass": chosen_volume_pass,
                    "close_pass": chosen_close_pass,
                    "break_pass": chosen_break_pass,
                    "mom_value": mom_val,
                    "vol_value": vol_val,
                    "close_strength_value": chosen_close_strength,
                    "break_distance_value": chosen_break_distance,
                    "structure_state_tf": structure_state_tf,
                    "structure_state_15m": structure_state_15m,
                    "structure_state_1h": structure_state_1h,
                }
                if DEBUG_LOGS:
                    _db_state_log(
                        symbol,
                        timeframe,
                        "pending_rearm_stored",
                        old_trade_id=pending.get("trade_id"),
                        old_setup_id=pending.get("setup_id"),
                        new_side=chosen_side,
                        new_bos_ts=last_ts,
                    )
                waiting_rows = [
                    r for r in scope_rows
                    if r.get("db_active_seen")
                    and r.get("db_active_status") == "nt-waiting"
                    and str(r.get("manage") or "") != "C"
                ]
                if waiting_rows:
                    if DEBUG_LOGS:
                        _db_state_log(
                            symbol,
                            timeframe,
                            "phase2_case2_cancel_request",
                            trade_id=pending.get("trade_id"),
                            setup_id=pending.get("setup_id"),
                            waiting_rows=len(waiting_rows),
                            waiting_leg_trades=",".join(
                                f"{_safe_int(r.get('leg'), 0)}-{_safe_int(r.get('trade'), 0)}"
                                for r in waiting_rows
                            ),
                        )
                    _bridge_update_rows(waiting_rows, {"manage": "C"}, "phase2_case2_cancel")
                    pending["cancel_requested"] = True
                elif DEBUG_LOGS:
                    _db_state_log(
                        symbol,
                        timeframe,
                        "phase2_case2_cancel_skipped_no_waiting_rows",
                        trade_id=pending.get("trade_id"),
                        setup_id=pending.get("setup_id"),
                        **_bridge_counts(scope_rows),
                    )
                # Do NOT clear pending_setup here based on the current BOS branch.
                # Cancel-confirmation is handled every candle above, independent
                # of whether another BOS happens again.
            elif DEBUG_LOGS:
                _db_state_log(
                    symbol,
                    timeframe,
                    "rearm_blocked_live_rows_present",
                    trade_id=pending.get("trade_id"),
                    setup_id=pending.get("setup_id"),
                    **_bridge_counts(scope_rows),
                )

        if not state.get("pending_setup"):
            rearm = state.get("pending_rearm_setup")
            # Do not publish a new Phase-1 setup while the current DB-owned setup
            # is still live or still awaiting Phase-3 cleanup confirmation.
            bridge_blocks_new_setup = False
            bridge_block_reason = None
            current_bridge_setup_id = state.get("bridge_current_setup_id")
            current_bridge_rows = _bridge_setup_rows(state, current_bridge_setup_id)
            current_bridge_meta = (state.get("bridge_setup_index") or {}).get(current_bridge_setup_id) or {}
            if current_bridge_rows:
                current_has_managing = any(r.get("db_active_status") == "nt-managing" for r in current_bridge_rows)
                current_waiting_uncancelled = any(
                    r.get("db_active_seen")
                    and r.get("db_active_status") == "nt-waiting"
                    and str(r.get("db_active_manage") or "") != "C"
                    for r in current_bridge_rows
                )
                current_live_phase_entered = bool(current_bridge_meta.get("bridge_live_phase_entered"))
                if current_has_managing:
                    bridge_blocks_new_setup = True
                    bridge_block_reason = "current_setup_live"
                elif current_live_phase_entered and current_waiting_uncancelled:
                    bridge_blocks_new_setup = True
                    bridge_block_reason = "phase3_cleanup_pending"
            if bridge_blocks_new_setup:
                if DEBUG_LOGS:
                    _db_state_log(
                        symbol, timeframe, "new_setup_blocked_by_current_bridge_setup",
                        bridge_current_setup_id=current_bridge_setup_id, reason=bridge_block_reason, **_bridge_counts(current_bridge_rows)
                    )
                rearm = None
            candidate = rearm or {
                "side": chosen_side,
                "bos_ts": last_ts,
                "bos_ts_et": last_ts_et,
                "entry_ref_swing_high": recent_high_price,
                "entry_ref_swing_high_ts": recent_high_ts,
                "entry_ref_swing_high_score": recent_high_pivot_score,
                "entry_ref_swing_low": recent_low_price,
                "entry_ref_swing_low_ts": recent_low_ts,
                "entry_ref_swing_low_score": recent_low_pivot_score,
                "score_total": chosen_score_total,
                "score_pass": chosen_score_pass,
                "momentum_pass": chosen_momentum_pass,
                "volume_pass": chosen_volume_pass,
                "close_pass": chosen_close_pass,
                "break_pass": chosen_break_pass,
                "mom_value": mom_val,
                "vol_value": vol_val,
                "close_strength_value": chosen_close_strength,
                "break_distance_value": chosen_break_distance,
                "structure_state_tf": structure_state_tf,
                "structure_state_15m": structure_state_15m,
                "structure_state_1h": structure_state_1h,
            } if (rearm is not None or not bridge_blocks_new_setup) else None
            if candidate is not None:
                state["trade_id_counter"] += 1
                trade_id = f"{symbol}_{timeframe}_{state['trade_id_counter']}"
                top_shares = ENTRY_LEG_SHARES * 2
                mid_shares = ENTRY_LEG_SHARES * 4
                bottom_shares = ENTRY_LEG_SHARES * 6
                total_shares = top_shares + mid_shares + bottom_shares
                state["pending_setup"] = {
                    "trade_id": trade_id,
                    "side": candidate.get("side"),
                    "bos_ts": candidate.get("bos_ts"),
                    "bos_ts_et": candidate.get("bos_ts_et"),
                    "shares_total": total_shares,
                    "entry_top_shares": top_shares,
                    "entry_mid_shares": mid_shares,
                    "entry_bottom_shares": bottom_shares,
                    "entry_ref_swing_high": candidate.get("entry_ref_swing_high"),
                    "entry_ref_swing_high_ts": candidate.get("entry_ref_swing_high_ts"),
                    "entry_ref_swing_high_score": candidate.get("entry_ref_swing_high_score"),
                    "entry_ref_swing_low": candidate.get("entry_ref_swing_low"),
                    "entry_ref_swing_low_ts": candidate.get("entry_ref_swing_low_ts"),
                    "entry_ref_swing_low_score": candidate.get("entry_ref_swing_low_score"),
                    "score_total": candidate.get("score_total"),
                    "score_pass": candidate.get("score_pass"),
                    "momentum_pass": candidate.get("momentum_pass"),
                    "volume_pass": candidate.get("volume_pass"),
                    "close_pass": candidate.get("close_pass"),
                    "break_pass": candidate.get("break_pass"),
                    "mom_value": candidate.get("mom_value"),
                    "vol_value": candidate.get("vol_value"),
                    "close_strength_value": candidate.get("close_strength_value"),
                    "break_distance_value": candidate.get("break_distance_value"),
                    "structure_state_tf": candidate.get("structure_state_tf"),
                    "structure_state_15m": candidate.get("structure_state_15m"),
                    "structure_state_1h": candidate.get("structure_state_1h"),
                    "fvg": None,
                    "setup_id": None,
                    "cancel_requested": False,
                    "notes": "awaiting_first_same_direction_fvg_after_bos",
                }
                if DEBUG_LOGS:
                    _db_state_log(
                        symbol,
                        timeframe,
                        "pending_setup_published",
                        trade_id=trade_id,
                        side=state["pending_setup"].get("side"),
                        bos_ts=state["pending_setup"].get("bos_ts"),
                        source="rearm" if rearm else "current_bos",
                    )
                state["pending_rearm_setup"] = None
                if DEBUG_LOGS:
                    print(
                        f"[BOS-FVG-DB] setup armed | Symbol={symbol} | TF={timeframe} | TradeID={trade_id} | "
                        f"Side={state['pending_setup'].get('side')} | BOS_TS={state['pending_setup'].get('bos_ts')} | "
                        f"BOSScore={_safe_float(state['pending_setup'].get('score_total'), 0.0):.2f}"
                    )

    # FVG discovery
    pending = state["pending_setup"]
    fvg_source = fvgs if isinstance(fvgs, list) else (last_candle.get("fvgs") if isinstance(last_candle.get("fvgs"), list) else [])
    if pending and not pending.get("fvg"):
        if DEBUG_LOGS:
            _db_state_log(
                symbol,
                timeframe,
                "fvg_search_start",
                trade_id=pending.get("trade_id"),
                side=pending.get("side"),
                bos_ts=pending.get("bos_ts"),
                fvg_pool_size=len(fvg_source or []),
            )
        bos_dt = _parse_ts(pending.get("bos_ts"))
        fvg = _select_first_post_bos_fvg(fvg_source or [], pending.get("side"), bos_dt)
        if fvg:
            fvg_score, trade_score, fvg_score_src, trade_score_src = _extract_fvg_score_fields(fvg)
            pending["fvg"] = {
                "created_ts": fvg.get("created_ts"),
                "direction": fvg.get("direction"),
                "low": _safe_float(fvg.get("low")),
                "high": _safe_float(fvg.get("high")),
                "filled": bool(fvg.get("filled", False)),
                "filled_ts": fvg.get("filled_ts"),
                "fvg_score": fvg_score,
                "trade_score": trade_score,
                "fvg_score_src": fvg_score_src,
                "trade_score_src": trade_score_src,
            }
            print(
                f"[BOS-FVG-DB] FVG selected | Symbol={symbol} | TF={timeframe} | TradeID={pending.get('trade_id')} | "
                f"Side={pending.get('side')} | FVG_TS={fvg.get('created_ts')} | Low={_safe_float(fvg.get('low'))} | High={_safe_float(fvg.get('high'))} | "
                f"FVGScore={fvg_score} | TradeScore={trade_score} | FVGScoreSrc={fvg_score_src} | TradeScoreSrc={trade_score_src} | "
                f"FVG={json.dumps(fvg, default=str, sort_keys=True)}"
            )
            if execution_enabled and live_mode:
                setup_id = _bridge_insert_rows(
                    state=state,
                    symbol=symbol,
                    timeframe=timeframe,
                    side=pending.get("side"),
                    version=BRIDGE_VERSION,
                    strategy_name="bos_fvg_ltf",
                    bos_ts=pending.get("bos_ts"),
                    fvg_high=_safe_float(fvg.get("high")),
                    fvg_low=_safe_float(fvg.get("low")),
                )
                pending["setup_id"] = setup_id or _deterministic_setup_id(
                    symbol,
                    timeframe,
                    pending.get("bos_ts"),
                    BRIDGE_VERSION,
                    pending.get("side"),
                    _safe_float(fvg.get("high")),
                    _safe_float(fvg.get("low")),
                )
                pending["notes"] = "fvg_selected_live_rows_created"
                if DEBUG_LOGS:
                    _db_state_log(
                        symbol,
                        timeframe,
                        "fvg_selected_and_rows_created",
                        trade_id=pending.get("trade_id"),
                        setup_id=pending.get("setup_id"),
                        fvg_ts=pending["fvg"].get("created_ts"),
                        fvg_low=pending["fvg"].get("low"),
                        fvg_high=pending["fvg"].get("high"),
                    )
            else:
                # Catch-up mode:
                # keep the pending setup + selected FVG, but do NOT materialize
                # Phase 1 rows yet. The first live candle can materialize later.
                pending["setup_id"] = None
                pending["notes"] = "fvg_selected_catchup_waiting_for_live_materialization"
                if DEBUG_LOGS:
                    _db_state_log(
                        symbol,
                        timeframe,
                        "fvg_selected_catchup_no_rows_created",
                        trade_id=pending.get("trade_id"),
                        fvg_ts=pending["fvg"].get("created_ts"),
                        fvg_low=pending["fvg"].get("low"),
                        fvg_high=pending["fvg"].get("high"),
                    )
        elif DEBUG_LOGS:
            _db_state_log(
                symbol,
                timeframe,
                "fvg_search_none",
                trade_id=pending.get("trade_id"),
                side=pending.get("side"),
                bos_ts=pending.get("bos_ts"),
                fvg_pool_size=len(fvg_source or []),
            )
    elif pending and pending.get("fvg"):
        pending["fvg"], _ = _refresh_selected_fvg_fields(pending.get("fvg"), fvg_source or [])
        if DEBUG_LOGS:
            _db_state_log(
                symbol,
                timeframe,
                "fvg_refresh_existing",
                trade_id=pending.get("trade_id"),
                setup_id=pending.get("setup_id"),
                fvg_ts=(pending.get("fvg") or {}).get("created_ts"),
                fvg_filled=bool((pending.get("fvg") or {}).get("filled", False)),
                trade_score=_safe_float((pending.get("fvg") or {}).get("trade_score")),
            )

    pending = state["pending_setup"]
    pending_setup_rows = _bridge_setup_rows(state, (pending or {}).get("setup_id"))
    if pending and pending.get("fvg"):
        if pending_setup_rows and not all(bool(r.get("db_new_insert_confirmed")) for r in pending_setup_rows):
            status = "pending_setup"
            skip_reason = "awaiting_new_trades_insert_confirmation"
            if DEBUG_LOGS:
                _db_state_log(
                    symbol,
                    timeframe,
                    "awaiting_new_trades_insert_confirmation",
                    trade_id=pending.get("trade_id"),
                    setup_id=pending.get("setup_id"),
                    confirmed_rows=sum(1 for r in pending_setup_rows if bool(r.get("db_new_insert_confirmed"))),
                    total_rows=len(pending_setup_rows),
                )

    bridge_setup_id = state.get("bridge_current_setup_id")
    bridge_rows = _bridge_setup_rows(state, bridge_setup_id)
    bridge_setup_meta = (state.get("bridge_setup_index") or {}).get(bridge_setup_id) or {}
    managing_trade1 = [r for r in bridge_rows if r.get("db_active_status") == "nt-managing" and _safe_int(r.get("trade"), 0) == 1]
    managing_trade2 = [r for r in bridge_rows if r.get("db_active_status") == "nt-managing" and _safe_int(r.get("trade"), 0) == 2]
    if managing_trade1 or managing_trade2:
        bridge_setup_meta["bridge_live_phase_entered"] = True
    managing_rows_all = managing_trade1 + managing_trade2
    avg_entry, avg_qty = _calc_avg_entry_from_rows(managing_rows_all)
    if bridge_setup_id and avg_entry is not None and avg_qty > 0:
        state["bridge_avg_entry"][bridge_setup_id] = avg_entry
        state["bridge_avg_qty"][bridge_setup_id] = avg_qty
        if DEBUG_LOGS:
            _db_state_log(
                symbol,
                timeframe,
                "avg_entry_updated",
                bridge_current_setup_id=bridge_setup_id,
                avg_entry=avg_entry,
                avg_qty=avg_qty,
                managing_trade1=len(managing_trade1),
                managing_trade2=len(managing_trade2),
            )

    # EOD close request at/after 15:56 ET for all surfaced active rows.
    if bridge_setup_id and bridge_rows and _is_eod_close_time(last_ts):
        if not state["bridge_eod_close_sent"].get(bridge_setup_id):
            active_rows_for_setup = [r for r in bridge_rows if bool(r.get("db_active_seen"))]
            if active_rows_for_setup:
                changed = _bridge_update_rows(active_rows_for_setup, {"manage": "C"}, "eod_close_1556")
                if changed:
                    state["bridge_eod_close_sent"][bridge_setup_id] = True
                    if DEBUG_LOGS:
                        _db_state_log(
                            symbol,
                            timeframe,
                            "eod_close_request_sent",
                            bridge_current_setup_id=bridge_setup_id,
                            active_rows=len(active_rows_for_setup),
                            changed_rows=changed,
                        )

    # Phase 3 SL management
    # Longs use swing lows; shorts use swing highs.
    # Trade-bucket behavior:
    #   - if trade1 managing:
    #       longs  -> ts newer AND swing low  > max(BE, last_trade1_sl)
    #                 trade1 rows -> swing low, trade2 rows -> BE
    #       shorts -> ts newer AND swing high < min(BE, last_trade1_sl)
    #                 trade1 rows -> swing high, trade2 rows -> BE
    #   - if no trade1 managing but trade2 managing:
    #       longs  -> ts only gate, trade2 rows -> swing low
    #       shorts -> ts only gate, trade2 rows -> swing high
    cp = bridge_rows[0].get("cp") if bridge_rows else None
    if cp == "call":
        be_price = state["bridge_avg_entry"].get(bridge_setup_id)
        if managing_trade1:
            swing_ts = recent_low_ts
            swing_level = recent_low_price
            swing_key = f"{bridge_setup_id}:swing_update_trade1"
            prev_swing_ts = state["bridge_last_swing_ts"].get(swing_key)
            prev_trade1_sl = None
            for row in managing_trade1:
                sl_val = _safe_float(row.get("sl_level"))
                if sl_val is None:
                    continue
                prev_trade1_sl = sl_val if prev_trade1_sl is None else max(prev_trade1_sl, sl_val)
            threshold = max(
                _safe_float(be_price, float("-inf")) if be_price is not None else float("-inf"),
                _safe_float(prev_trade1_sl, float("-inf")) if prev_trade1_sl is not None else float("-inf"),
            )
            if swing_ts and swing_level is not None and (prev_swing_ts is None or swing_ts > prev_swing_ts) and swing_level > threshold:
                state["bridge_last_swing_ts"][swing_key] = swing_ts
                _bridge_update_rows(managing_trade1, {"sl_level": swing_level}, "swing_update_trade1")
                if managing_trade2 and be_price is not None:
                    _bridge_update_rows(managing_trade2, {"sl_level": be_price}, "be_update_trade2")
        elif managing_trade2:
            swing_ts = recent_low_ts
            swing_level = recent_low_price
            swing_key = f"{bridge_setup_id}:swing_update_trade2_only"
            prev_swing_ts = state["bridge_last_swing_ts"].get(swing_key)
            if swing_ts and swing_level is not None and (prev_swing_ts is None or swing_ts > prev_swing_ts):
                state["bridge_last_swing_ts"][swing_key] = swing_ts
                _bridge_update_rows(managing_trade2, {"sl_level": swing_level}, "swing_update_trade2_only")
    elif cp == "put":
        be_price = state["bridge_avg_entry"].get(bridge_setup_id)
        if managing_trade1:
            swing_ts = recent_high_ts
            swing_level = recent_high_price
            swing_key = f"{bridge_setup_id}:swing_update_trade1"
            prev_swing_ts = state["bridge_last_swing_ts"].get(swing_key)
            prev_trade1_sl = None
            for row in managing_trade1:
                sl_val = _safe_float(row.get("sl_level"))
                if sl_val is None:
                    continue
                prev_trade1_sl = sl_val if prev_trade1_sl is None else min(prev_trade1_sl, sl_val)
            threshold = min(
                _safe_float(be_price, float("inf")) if be_price is not None else float("inf"),
                _safe_float(prev_trade1_sl, float("inf")) if prev_trade1_sl is not None else float("inf"),
            )
            if swing_ts and swing_level is not None and (prev_swing_ts is None or swing_ts > prev_swing_ts) and swing_level < threshold:
                state["bridge_last_swing_ts"][swing_key] = swing_ts
                _bridge_update_rows(managing_trade1, {"sl_level": swing_level}, "swing_update_trade1")
                if managing_trade2 and be_price is not None:
                    _bridge_update_rows(managing_trade2, {"sl_level": be_price}, "be_update_trade2")
        elif managing_trade2:
            swing_ts = recent_high_ts
            swing_level = recent_high_price
            swing_key = f"{bridge_setup_id}:swing_update_trade2_only"
            prev_swing_ts = state["bridge_last_swing_ts"].get(swing_key)
            if swing_ts and swing_level is not None and (prev_swing_ts is None or swing_ts > prev_swing_ts):
                state["bridge_last_swing_ts"][swing_key] = swing_ts
                _bridge_update_rows(managing_trade2, {"sl_level": swing_level}, "swing_update_trade2_only")
    if bridge_rows:
        has_managing = any(r.get("db_active_status") == "nt-managing" for r in bridge_rows)
        has_active = any(bool(r.get("db_active_seen")) for r in bridge_rows)
        live_phase_entered = bool(bridge_setup_meta.get("bridge_live_phase_entered"))
        if DEBUG_LOGS:
            _db_state_log(
                symbol,
                timeframe,
                "bridge_current_setup_status",
                bridge_current_setup_id=bridge_setup_id,
                live_phase_entered=live_phase_entered,
                **_bridge_counts(bridge_rows),
            )
        if live_phase_entered and has_active and not has_managing:
            waiting_rows = [r for r in bridge_rows if r.get("db_active_status") == "nt-waiting"]
            waiting_uncancelled = [r for r in waiting_rows if str(r.get("db_active_manage") or "") != "C"]
            cancel_rows = [r for r in bridge_rows if r.get("db_active_status") == "nt-waiting"]
            changed = _bridge_update_rows(cancel_rows, {"manage": "C"}, "phase3_no_live_rows")
            if changed and DEBUG_LOGS:
                _db_state_log(symbol, timeframe, "phase3_cancel_request_sent", bridge_current_setup_id=bridge_setup_id, waiting_rows=len(cancel_rows))

            # Do NOT rely on canceled rows remaining queryable after manage="C".
            # If there are no live rows left, and either no waiting rows remain or
            # no waiting rows remain uncancelled, Phase 3 cleanup is complete.
            if not waiting_rows:
                if DEBUG_LOGS:
                    _db_state_log(
                        symbol, timeframe, "phase3_complete_no_rows_remaining",
                        bridge_current_setup_id=bridge_setup_id
                    )
                bridge_setup_meta["bridge_live_phase_entered"] = False
                if state.get("bridge_current_setup_id") == bridge_setup_id:
                    state["bridge_current_setup_id"] = None
                state["bridge_eod_close_sent"].pop(bridge_setup_id, None)
            elif not waiting_uncancelled:
                if DEBUG_LOGS:
                    _db_state_log(
                        symbol, timeframe, "phase3_cleanup_confirmed",
                        bridge_current_setup_id=bridge_setup_id,
                        waiting_rows=len(waiting_rows)
                    )
                bridge_setup_meta["bridge_live_phase_entered"] = False
                if state.get("bridge_current_setup_id") == bridge_setup_id:
                    state["bridge_current_setup_id"] = None
                state["bridge_eod_close_sent"].pop(bridge_setup_id, None)

            else:
                # Keep Phase 3 marked entered until cleanup is actually confirmed
                # by disappearance / no waiting rows / all waiting rows surfaced
                # as already canceled in DB.
                bridge_setup_meta["bridge_live_phase_entered"] = True

    if not cfg["enabled"]:
        status = "disabled"
        skip_reason = "bos_score_disabled"
    elif any(r.get("db_active_status") == "nt-managing" for r in bridge_rows):
        status = "live"
    elif state.get("pending_setup"):
        status = "pending_setup"
    elif state.get("bridge_current_setup_id") and bool(bridge_setup_meta.get("bridge_live_phase_entered")):
        status = "live"

    if DEBUG_LOGS:
        _db_state_log(
            symbol,
            timeframe,
            "eval_end",
            status=status,
            skip_reason=skip_reason,
            **_pending_summary(state.get("pending_setup")),
            **_rearm_summary(state.get("pending_rearm_setup")),
            bridge_current_setup_id=state.get("bridge_current_setup_id"),
        )

    state["signal_id_counter"] += 1
    signal = {
        "signal_id": state["signal_id_counter"],
        "ts": last_ts,
        "ts_et": last_ts_et,
        "symbol": symbol,
        "timeframe": timeframe,
        "signal_side": chosen_side,
        "bos_detected": chosen_bos_detected,
        "long_bos_detected": long_bos_detected,
        "short_bos_detected": short_bos_detected,
        "score_total": chosen_score_total,
        "score_threshold": cfg["score_min"],
        "trade_score_threshold": cfg.get("trade_score_min", 0.0),
        "score_pass": chosen_score_pass,
        "mom_pass": chosen_momentum_pass,
        "vol_pass": chosen_volume_pass,
        "close_pass": chosen_close_pass,
        "break_pass": chosen_break_pass,
        "mom_value": mom_val,
        "vol_value": vol_val,
        "close_strength_value": chosen_close_strength,
        "break_distance_value": chosen_break_distance,
        "skip_reason": skip_reason,
    }
    state["signals"].append(signal)

    snapshot = {
        "id": "bos_fvg_ltf",
        "symbol": symbol,
        "timeframe": timeframe,
        "last_eval_ts": last_ts,
        "last_eval_ts_et": last_ts_et,
        "status": status,
        "skip_reason": skip_reason,
        "pending_setup_side": (state["pending_setup"] or {}).get("side"),
        "pending_setup": deepcopy(state["pending_setup"]),
        "last_signal": deepcopy(state["signals"][-1] if state["signals"] else None),
        "signal_count": len(state["signals"]),
        "bridge_current_setup_id": state.get("bridge_current_setup_id"),
        "live_trade_rows": deepcopy(state["bridge_rows"]),
        "execution_enabled": execution_enabled,
        "live_mode": live_mode,
    }
    state["latest_snapshot"] = snapshot
    return snapshot
