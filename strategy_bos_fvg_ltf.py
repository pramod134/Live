import os
import json
import hashlib
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from db_worker import db_insert_raw, active_trades_checker, active_cleanup, db_updater



_BOS_FVG_LTF_STATE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_ET = ZoneInfo("America/New_York")
# This module is bridge-only: it manages BOS/FVG setup orchestration and DB trade-row lifecycle, but does not perform internal trade simulation.
DEBUG_LOGS = str(os.getenv("BOS_FVG_DEBUG_LOGS", "0")).strip().lower() in {"1", "true", "t", "yes", "y", "on"}
ENTRY_LEG_SHARES = 100
BRIDGE_VERSION = (os.getenv("BOS_FVG_LTF_VERSION") or "v1").strip() or "v1"
STRATEGY_NAME = "bos_fvg_ltf"
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


def _snapshot_serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, set):
        return {"__type__": "set", "value": [_snapshot_serialize(v) for v in sorted(value, key=lambda x: str(x))]}
    if isinstance(value, tuple):
        return {"__type__": "tuple", "value": [_snapshot_serialize(v) for v in value]}
    if isinstance(value, list):
        return [_snapshot_serialize(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _snapshot_serialize(v) for k, v in value.items()}
    return value


def _snapshot_deserialize(value: Any) -> Any:
    if isinstance(value, list):
        return [_snapshot_deserialize(v) for v in value]
    if isinstance(value, dict):
        marker = value.get("__type__")
        if marker == "datetime":
            try:
                return datetime.fromisoformat(str(value.get("value") or ""))
            except Exception:
                return value.get("value")
        if marker == "set":
            return set(_snapshot_deserialize(v) for v in (value.get("value") or []))
        if marker == "tuple":
            return tuple(_snapshot_deserialize(v) for v in (value.get("value") or []))
        return {k: _snapshot_deserialize(v) for k, v in value.items()}
    return value


def export_bridge_snapshot_for_symbol(symbol: str) -> Dict[str, Any]:
    sym = str(symbol or "").upper()
    out: Dict[str, Any] = {
        "symbol": sym,
        "runtime_mode": get_bridge_runtime_mode(),
        "states": {},
    }
    for (row_symbol, timeframe), state in _BOS_FVG_LTF_STATE.items():
        if str(row_symbol or "").upper() != sym:
            continue
        out["states"][str(timeframe)] = _snapshot_serialize(deepcopy(state))
    return out


def restore_bridge_snapshot_for_symbol(symbol: str, snapshot: Dict[str, Any]) -> None:
    sym = str(symbol or "").upper()
    if not isinstance(snapshot, dict):
        return
    runtime_mode = snapshot.get("runtime_mode")
    if isinstance(runtime_mode, dict):
        _RUNTIME_MODE["execution_enabled"] = bool(runtime_mode.get("execution_enabled", True))
        _RUNTIME_MODE["live_mode"] = bool(runtime_mode.get("live_mode", True))
    states = snapshot.get("states")
    if not isinstance(states, dict):
        return
    for timeframe, raw_state in states.items():
        key = (sym, str(timeframe))
        restored = _snapshot_deserialize(raw_state)
        if isinstance(restored, dict):
            _BOS_FVG_LTF_STATE[key] = restored


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


def _insert_day_end_time_utc_iso() -> str:
    now_et = datetime.now(_ET)
    end_et = now_et.replace(hour=15, minute=56, second=0, microsecond=0)
    return end_et.astimezone(ZoneInfo("UTC")).isoformat()


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
            "mode": "sim",
            "phase": None,
            "strategy": STRATEGY_NAME,
            "version": BRIDGE_VERSION,
            "symbol": symbol,
            "timeframe": timeframe,
            "setup_id": None,
            "side": None,
            "bos_info": None,
            "fvg_info": None,
            "swing_info": None,
            "new_bos_info": None,
            "pending_setup": None,
            "pending_rearm_setup": None,
            "deferred_bos": None,
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
    strategy_name: str,
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
        strategy_name,
        version,
        symbol,
        timeframe,
        bos_ts_s,
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
    setup_id = _deterministic_setup_id(strategy_name, symbol, timeframe, bos_ts, version, side, fvg_high, fvg_low)
    if setup_id in state["bridge_setup_index"]:
        return None

    cp = "call" if side == "long" else "put"
    sl_cond = "cb" if side == "long" else "ca"

    PRICE_BROADEN = 0.10
    MIN_TRADE3_RISK = 0.60
    TRADE3_RR_BY_LEG = {
        1: 1.0,
        2: 1.5,
        3: 2.0,
    }

    raw_fvg_high = _safe_float(fvg_high)
    raw_fvg_low = _safe_float(fvg_low)
    end_time = _insert_day_end_time_utc_iso()

    fvg_mid = None
    if raw_fvg_high is not None and raw_fvg_low is not None:
        fvg_mid = (raw_fvg_high + raw_fvg_low) / 2.0

    # 3-leg ladder with broadened entry/SL levels:
    # longs  -> entries at high+0.10, mid+0.10, low+0.10; stop at low-0.10
    # shorts -> entries at low-0.10,  mid-0.10, high-0.10; stop at high+0.10
    if side == "long":
        sl_level = (raw_fvg_low - PRICE_BROADEN) if raw_fvg_low is not None else None
        leg_levels = [
            (1, (raw_fvg_high + PRICE_BROADEN) if raw_fvg_high is not None else None, 1),
            (2, (fvg_mid + PRICE_BROADEN) if fvg_mid is not None else None, 2),
            (3, (raw_fvg_low + PRICE_BROADEN) if raw_fvg_low is not None else None, 3),
        ]
    else:
        sl_level = (raw_fvg_high + PRICE_BROADEN) if raw_fvg_high is not None else None
        leg_levels = [
            (1, (raw_fvg_low - PRICE_BROADEN) if raw_fvg_low is not None else None, 1),
            (2, (fvg_mid - PRICE_BROADEN) if fvg_mid is not None else None, 2),
            (3, (raw_fvg_high - PRICE_BROADEN) if raw_fvg_high is not None else None, 3),
        ]

    rows: List[Dict[str, Any]] = []
    for leg, entry_level, qty_per_trade in leg_levels:
        for trade in (1, 2, 3):
            tp_type = None
            tp_level = None

            # Trade 3 only: fixed TP, set-and-done. No special manage flag needed;
            # existing maintenance code only touches trade 1 and trade 2.
            if trade == 3 and entry_level is not None and sl_level is not None:
                effective_risk = max(abs(entry_level - sl_level), MIN_TRADE3_RISK)
                rr_mult = TRADE3_RR_BY_LEG.get(leg, 1.0)
                if side == "long":
                    tp_level = entry_level + (rr_mult * effective_risk)
                else:
                    tp_level = entry_level - (rr_mult * effective_risk)
                tp_type = "equity"

            tags = [
                f"strategy:{strategy_name}",
                f"version:{version}",
                f"id:{setup_id}",
                f"tf:{timeframe}",
                f"leg:{leg}",
                f"trade:{trade}",
            ]
            if trade == 3:
                tags.append("exit:fixed_tp")

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
                "tp_type": tp_type,
                "tp_level": tp_level,
                "end_time": end_time,
                "status": "nt-waiting",
                "manage": None,
                "db_new_insert_confirmed": False,
                "db_active_seen": False,
                "db_active_status": None,
                "db_active_manage": None,
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


def _deferred_bos_summary(deferred: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(deferred, dict):
        return {
            "deferred_bos_exists": False,
            "deferred_bos_side": None,
            "deferred_bos_ts": None,
        }
    return {
        "deferred_bos_exists": True,
        "deferred_bos_side": deferred.get("side"),
        "deferred_bos_ts": deferred.get("bos_ts"),
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


def _begin_manage_c_cleanup(
    state: Dict[str, Any],
    *,
    setup_id: str,
    source: str,
    rows: List[Dict[str, Any]],
    clear_pending_setup: bool,
) -> None:
    setup_meta = (state.get("bridge_setup_index") or {}).get(setup_id) or {}
    targets = {
        (_safe_int(r.get("leg"), 0), _safe_int(r.get("trade"), 0))
        for r in (rows or [])
        if _safe_int(r.get("leg"), 0) > 0 and _safe_int(r.get("trade"), 0) > 0
    }
    if not targets:
        return
    setup_meta["cleanup_manage_c_pending"] = {
        "source": source,
        "targets": targets,
        "acked": set(),
        "clear_pending_setup": bool(clear_pending_setup),
    }


def _finalize_manage_c_cleanup(
    state: Dict[str, Any],
    *,
    setup_id: str,
) -> None:
    setup_meta = (state.get("bridge_setup_index") or {}).get(setup_id) or {}
    cleanup_meta = setup_meta.get("cleanup_manage_c_pending") or {}
    if cleanup_meta.get("clear_pending_setup"):
        pending = state.get("pending_setup")
        if pending and pending.get("setup_id") == setup_id:
            state["pending_setup"] = None
    setup_meta["bridge_live_phase_entered"] = False
    if state.get("bridge_current_setup_id") == setup_id:
        state["bridge_current_setup_id"] = None
    state["bridge_eod_close_sent"].pop(setup_id, None)
    setup_meta.pop("cleanup_manage_c_pending", None)
    rearm = state.get("pending_rearm_setup")
    if rearm and not state.get("pending_setup"):
        existing_rows = setup_meta.get("rows") or []
        symbol = str((existing_rows[0] if existing_rows else {}).get("symbol") or "")
        timeframe = str((existing_rows[0] if existing_rows else {}).get("entry_tf") or "")
        if symbol and timeframe:
            state["trade_id_counter"] += 1
            trade_id = f"{symbol}_{timeframe}_{state['trade_id_counter']}"
            top_shares = ENTRY_LEG_SHARES * 2
            mid_shares = ENTRY_LEG_SHARES * 4
            bottom_shares = ENTRY_LEG_SHARES * 6
            total_shares = top_shares + mid_shares + bottom_shares
            state["pending_setup"] = {
                "trade_id": trade_id,
                "side": rearm.get("side"),
                "bos_ts": rearm.get("bos_ts"),
                "bos_ts_et": rearm.get("bos_ts_et"),
                "shares_total": total_shares,
                "entry_top_shares": top_shares,
                "entry_mid_shares": mid_shares,
                "entry_bottom_shares": bottom_shares,
                "entry_ref_swing_high": rearm.get("entry_ref_swing_high"),
                "entry_ref_swing_high_ts": rearm.get("entry_ref_swing_high_ts"),
                "entry_ref_swing_high_score": rearm.get("entry_ref_swing_high_score"),
                "entry_ref_swing_low": rearm.get("entry_ref_swing_low"),
                "entry_ref_swing_low_ts": rearm.get("entry_ref_swing_low_ts"),
                "entry_ref_swing_low_score": rearm.get("entry_ref_swing_low_score"),
                "score_total": rearm.get("score_total"),
                "score_pass": rearm.get("score_pass"),
                "momentum_pass": rearm.get("momentum_pass"),
                "volume_pass": rearm.get("volume_pass"),
                "close_pass": rearm.get("close_pass"),
                "break_pass": rearm.get("break_pass"),
                "mom_value": rearm.get("mom_value"),
                "vol_value": rearm.get("vol_value"),
                "close_strength_value": rearm.get("close_strength_value"),
                "break_distance_value": rearm.get("break_distance_value"),
                "structure_state_tf": rearm.get("structure_state_tf"),
                "structure_state_15m": rearm.get("structure_state_15m"),
                "structure_state_1h": rearm.get("structure_state_1h"),
                "fvg": None,
                "setup_id": None,
                "cancel_requested": False,
                "notes": "promoted_from_rearm_after_manage_c_db_applied",
            }
            state["pending_rearm_setup"] = None


def get_live_bridge_rows() -> List[Dict[str, Any]]:
    return []


def _direct_state_log(stage: str, state: Dict[str, Any], **extra: Any) -> None:
    parts = [
        f"[BOS-FVG-DB][STATE] Stage={stage}",
        f"Symbol={state.get('symbol')}",
        f"TF={state.get('timeframe')}",
        f"mode={state.get('mode')}",
        f"phase={state.get('phase')}",
        f"setup_id={state.get('setup_id')}",
        f"side={state.get('side')}",
        f"bos={state.get('bos_info') is not None}",
        f"bos_ts={(state.get('bos_info') or {}).get('bos_ts')}",
        f"fvg={state.get('fvg_info') is not None}",
        f"new_bos={state.get('new_bos_info') is not None}",
        f"latest_bull_fvg_ts={state.get('latest_bull_fvg_ts')}",
        f"latest_bull_fvg_high={state.get('latest_bull_fvg_high')}",
        f"latest_bull_fvg_low={state.get('latest_bull_fvg_low')}",
        f"latest_bear_fvg_ts={state.get('latest_bear_fvg_ts')}",
        f"latest_bear_fvg_high={state.get('latest_bear_fvg_high')}",
        f"latest_bear_fvg_low={state.get('latest_bear_fvg_low')}",
    ]
    for k, v in extra.items():
        parts.append(f"{k}={_log_value(v)}")
    print(" | ".join(parts))



def _new_trade_payloads(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    columns = [
        "symbol",
        "asset_type",
        "cp",
        "strike",
        "expiry",
        "qty",
        "entry_type",
        "entry_cond",
        "entry_level",
        "entry_tf",
        "sl_type",
        "sl_cond",
        "sl_level",
        "sl_tf",
        "tp_type",
        "tp_level",
        "note",
        "trade_type",
        "entry_time",
        "end_time",
        "tags",
    ]

    payloads: List[Dict[str, Any]] = []

    for row in rows or []:
        setup_id = str(row.get("setup_id") or "").strip()

        payload = {col: None for col in columns}
        payload["note"] = f"bridge:{setup_id}" if setup_id else "bridge:"
        payload["trade_type"] = "swing"

        for col in columns:
            if col in {"note", "trade_type"}:
                continue
            payload[col] = row.get(col)

        payloads.append(payload)

    return payloads




def _clear_direct_setup(state: Dict[str, Any]) -> None:
    state["phase"] = "phase1_setup"
    state["setup_id"] = None
    state["side"] = None
    state["bos_info"] = None
    state["fvg_info"] = None
    state["swing_info"] = None
    state["new_bos_info"] = None
    state["bridge_current_setup_id"] = None


def _active_tag_value(row: Dict[str, Any], prefix: str) -> Optional[str]:
    tags = row.get("tags") or []
    want = f"{prefix}:"
    for tag in tags:
        if isinstance(tag, str) and tag.startswith(want):
            return tag.split(":", 1)[1].strip() or None
    return None


def _bridge_rows_from_active(
    state: Dict[str, Any],
    setup_id: str,
    active_rows: List[Dict[str, Any]],
    *,
    trade_number: Optional[int] = None,
) -> List[Dict[str, Any]]:
    active_keys = set()
    for row in active_rows or []:
        is_managing = (
            str(row.get("manage") or "") == "O"
            or (
                str(row.get("status") or "") == "nt-managing"
                and str(row.get("manage") or "") == "Y"
            )
        )
        if not is_managing:
            continue
        leg = _safe_int(_active_tag_value(row, "leg"), 0)
        trade = _safe_int(_active_tag_value(row, "trade"), 0)
        if leg <= 0 or trade <= 0:
            continue
        if trade_number is not None and trade != trade_number:
            continue
        active_keys.add((leg, trade))

    return [
        r for r in _bridge_setup_rows(state, setup_id)
        if (_safe_int(r.get("leg"), 0), _safe_int(r.get("trade"), 0)) in active_keys
    ]


def _patch_sl_rows(state: Dict[str, Any], rows: List[Dict[str, Any]], sl_level: float, reason: str) -> None:
    for row in rows or []:
        if row.get("sl_level") == sl_level:
            continue
        row["sl_level"] = sl_level
        _bridge_trade_log("UPDATE", row, reason)
        db_updater(
            strategy=state["strategy"],
            version=state["version"],
            setup_id=state["setup_id"],
            leg=row.get("leg"),
            trade=row.get("trade"),
            sl_level=sl_level,
            log_label=state["strategy"],
        )


def _direct_snapshot(
    state: Dict[str, Any],
    *,
    last_ts: Optional[str],
    last_ts_et: Optional[str],
    status: str,
    skip_reason: str,
) -> Dict[str, Any]:
    snapshot = {
        "id": state.get("strategy"),
        "symbol": state.get("symbol"),
        "timeframe": state.get("timeframe"),
        "last_eval_ts": last_ts,
        "last_eval_ts_et": last_ts_et,
        "status": status,
        "skip_reason": skip_reason,
        "phase": state.get("phase"),
        "setup_id": state.get("setup_id"),
        "side": state.get("side"),
        "bos_info": deepcopy(state.get("bos_info")),
        "fvg_info": deepcopy(state.get("fvg_info")),
        "new_bos_info": deepcopy(state.get("new_bos_info")),
        "signal_count": len(state.get("signals") or []),
        "last_signal": deepcopy((state.get("signals") or [])[-1] if state.get("signals") else None),
        "execution_enabled": state.get("mode") == "live",
        "live_mode": state.get("mode") == "live",
    }
    state["latest_snapshot"] = snapshot
    _direct_state_log("eval_return", state, status=status, skip_reason=skip_reason)
    return snapshot


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


def apply_live_bridge_manage_c_db_applied(setup_id: str, leg: Any, trade: Any) -> None:
    setup_id_s = str(setup_id or "").strip()
    leg_i = _safe_int(leg, 0)
    trade_i = _safe_int(trade, 0)
    if not setup_id_s or leg_i <= 0 or trade_i <= 0:
        return
    for state in _BOS_FVG_LTF_STATE.values():
        setup_meta = (state.get("bridge_setup_index") or {}).get(setup_id_s) or {}
        cleanup_meta = setup_meta.get("cleanup_manage_c_pending")
        if not cleanup_meta:
            continue
        targets = cleanup_meta.get("targets") or set()
        if (leg_i, trade_i) not in targets:
            continue
        acked = cleanup_meta.setdefault("acked", set())
        acked.add((leg_i, trade_i))
        if acked >= targets:
            _finalize_manage_c_cleanup(state, setup_id=setup_id_s)


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


def _latest_fvg_levels_for_log(fvgs: List[Dict[str, Any]]) -> Dict[str, Any]:
    latest: Dict[str, Tuple[datetime, Dict[str, Any]]] = {}
    for fvg in fvgs or []:
        side = _normalize_fvg_direction(fvg.get("direction"))
        if side not in {"bull", "bear"}:
            continue
        created_dt = _parse_ts(fvg.get("created_ts"))
        if created_dt is None:
            continue
        prev = latest.get(side)
        if prev is None or created_dt > prev[0]:
            latest[side] = (created_dt, fvg)

    bull = (latest.get("bull") or (None, {}))[1]
    bear = (latest.get("bear") or (None, {}))[1]
    return {
        "latest_bull_fvg_ts": bull.get("created_ts"),
        "latest_bull_fvg_high": _safe_float(bull.get("high")),
        "latest_bull_fvg_low": _safe_float(bull.get("low")),
        "latest_bear_fvg_ts": bear.get("created_ts"),
        "latest_bear_fvg_high": _safe_float(bear.get("high")),
        "latest_bear_fvg_low": _safe_float(bear.get("low")),
    }


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
        print(
            f"[BOS-FVG-DB] BOS detected LONG | Symbol={symbol} | TF={timeframe} | Break={recent_high_price} | "
            f"RefSwingTS={recent_high_ts} | BOSTS={last_ts}"
        )
    if short_bos_detected and swing_low_key:
        state["broken_swing_lows"].add(swing_low_key)
        print(
            f"[BOS-FVG-DB] BOS detected SHORT | Symbol={symbol} | TF={timeframe} | Break={recent_low_price} | "
            f"RefSwingTS={recent_low_ts} | BOSTS={last_ts}"
        )

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

    candidate_bos: Optional[Dict[str, Any]] = None
    if cfg["enabled"] and chosen_bos_detected and chosen_score_pass:
        candidate_bos = {
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

    status = "idle"
    skip_reason = ""

    if DEBUG_LOGS:
        pending_dbg = _pending_summary(state.get("pending_setup"))
        rearm_dbg = _rearm_summary(state.get("pending_rearm_setup"))
        deferred_dbg = _deferred_bos_summary(state.get("deferred_bos"))
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
            **deferred_dbg,
        )

    state["mode"] = "live" if execution_enabled and live_mode else "sim"

    fvg_source = fvgs if isinstance(fvgs, list) else (last_candle.get("fvgs") if isinstance(last_candle.get("fvgs"), list) else [])
    state.update(_latest_fvg_levels_for_log(fvg_source or []))

    # ------------------------------------------------------------
    # Direct DB lifecycle mode.
    # BOS/FVG detection above this point is unchanged.
    # ------------------------------------------------------------

    if state["mode"] == "sim":
        state["phase"] = None
        state["setup_id"] = None
        state["new_bos_info"] = None

        if candidate_bos is not None:
            state["bos_info"] = deepcopy(candidate_bos)
            state["side"] = candidate_bos.get("side")
            state["swing_info"] = {
                "entry_ref_swing_high": candidate_bos.get("entry_ref_swing_high"),
                "entry_ref_swing_high_ts": candidate_bos.get("entry_ref_swing_high_ts"),
                "entry_ref_swing_high_score": candidate_bos.get("entry_ref_swing_high_score"),
                "entry_ref_swing_low": candidate_bos.get("entry_ref_swing_low"),
                "entry_ref_swing_low_ts": candidate_bos.get("entry_ref_swing_low_ts"),
                "entry_ref_swing_low_score": candidate_bos.get("entry_ref_swing_low_score"),
            }
            state["fvg_info"] = None
            _direct_state_log("sim_bos_updated", state, bos_ts=candidate_bos.get("bos_ts"))

        if state.get("bos_info") and not state.get("fvg_info"):
            bos_dt = _parse_ts((state.get("bos_info") or {}).get("bos_ts"))
            fvg = _select_first_post_bos_fvg(fvg_source or [], state.get("side"), bos_dt)
            if fvg:
                fvg_score, trade_score, fvg_score_src, trade_score_src = _extract_fvg_score_fields(fvg)
                state["fvg_info"] = {
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
                _direct_state_log("sim_fvg_selected", state, fvg_ts=fvg.get("created_ts"))

        return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="sim", skip_reason="sim_mode")

    if state["mode"] == "live" and state.get("phase") is None:
        state["phase"] = "phase1_setup"
        _direct_state_log("live_start", state)

    if state.get("phase") == "phase1_setup" and candidate_bos is not None:
        if (
            not state.get("bos_info")
            or state.get("setup_id") is None
            or (state.get("bos_info") or {}).get("bos_ts") != candidate_bos.get("bos_ts")
            or (state.get("bos_info") or {}).get("side") != candidate_bos.get("side")
        ):
            state["bos_info"] = deepcopy(candidate_bos)
            state["side"] = candidate_bos.get("side")
            state["swing_info"] = {
                "entry_ref_swing_high": candidate_bos.get("entry_ref_swing_high"),
                "entry_ref_swing_high_ts": candidate_bos.get("entry_ref_swing_high_ts"),
                "entry_ref_swing_high_score": candidate_bos.get("entry_ref_swing_high_score"),
                "entry_ref_swing_low": candidate_bos.get("entry_ref_swing_low"),
                "entry_ref_swing_low_ts": candidate_bos.get("entry_ref_swing_low_ts"),
                "entry_ref_swing_low_score": candidate_bos.get("entry_ref_swing_low_score"),
            }
            state["fvg_info"] = None
            _direct_state_log("phase1_bos_updated", state, bos_ts=candidate_bos.get("bos_ts"))

    if state.get("phase") == "phase1_setup":
        if not state.get("bos_info") or not state.get("fvg_info"):
            return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="pending_setup", skip_reason="phase1_waiting_for_bos_or_fvg")

        setup_id = _deterministic_setup_id(
            state["strategy"],
            symbol,
            timeframe,
            (state["bos_info"] or {}).get("bos_ts"),
            BRIDGE_VERSION,
            state.get("side"),
            _safe_float((state["fvg_info"] or {}).get("high")),
            _safe_float((state["fvg_info"] or {}).get("low")),
        )
        state["setup_id"] = setup_id

        existing_rows = _bridge_setup_rows(state, setup_id)
        if existing_rows:
            rows = existing_rows
        else:
            _bridge_insert_rows(
                state=state,
                symbol=symbol,
                timeframe=timeframe,
                side=state.get("side"),
                version=BRIDGE_VERSION,
                strategy_name=state["strategy"],
                bos_ts=(state["bos_info"] or {}).get("bos_ts"),
                fvg_high=_safe_float((state["fvg_info"] or {}).get("high")),
                fvg_low=_safe_float((state["fvg_info"] or {}).get("low")),
            )
            rows = _bridge_setup_rows(state, setup_id)

        payloads = _new_trade_payloads(rows)
        _direct_state_log("phase1_insert_attempt", state, rows=len(payloads))
        insert_result = db_insert_raw(
            table="new_trades",
            payload=payloads,
            log_label=state["strategy"],
        )
        if insert_result.get("success"):
            state["phase"] = "phase2_waiting"
            _direct_state_log("phase1_insert_success", state, attempts=insert_result.get("attempts"))
            return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="pending_setup", skip_reason="phase2_waiting")

        _direct_state_log("phase1_insert_failed", state, error=insert_result.get("error"))
        return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="pending_setup", skip_reason="phase1_insert_failed")

    if state.get("phase") == "phase2_waiting":
        if candidate_bos is not None:
            state["new_bos_info"] = deepcopy(candidate_bos)
            _direct_state_log("phase2_new_bos_detected", state, bos_ts=candidate_bos.get("bos_ts"))

        active = active_trades_checker(
            strategy=state["strategy"],
            version=state["version"],
            setup_id=state["setup_id"],
            log_label=state["strategy"],
        )
        if not active.get("success"):
            _direct_state_log("phase2_db_error", state, error=active.get("error"))
            return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="pending_setup", skip_reason="phase2_db_error")

        if active.get("managing_present"):
            state["phase"] = "phase3_live"
            state["new_bos_info"] = None
            _direct_state_log("phase2_to_phase3", state, managing_qty=active.get("managing_qty"))
            return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="live", skip_reason="")

        if state.get("new_bos_info"):
            _direct_state_log("phase2_cleanup_attempt", state)
            cleanup = active_cleanup(
                strategy=state["strategy"],
                version=state["version"],
                setup_id=state["setup_id"],
                log_label=state["strategy"],
            )
            if cleanup.get("success"):
                state["bos_info"] = deepcopy(state["new_bos_info"])
                state["side"] = state["bos_info"].get("side")
                state["swing_info"] = {
                    "entry_ref_swing_high": state["bos_info"].get("entry_ref_swing_high"),
                    "entry_ref_swing_high_ts": state["bos_info"].get("entry_ref_swing_high_ts"),
                    "entry_ref_swing_low": state["bos_info"].get("entry_ref_swing_low"),
                    "entry_ref_swing_low_ts": state["bos_info"].get("entry_ref_swing_low_ts"),
                }
                state["fvg_info"] = None
                state["setup_id"] = None
                state["new_bos_info"] = None
                state["phase"] = "phase1_setup"
                state["bridge_current_setup_id"] = None
                _direct_state_log("phase2_cleanup_success", state)
                return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="pending_setup", skip_reason="cleanup_restart")

            _direct_state_log("phase2_cleanup_failed", state, error=cleanup.get("error"))
            return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="pending_setup", skip_reason="cleanup_failed")

        return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="pending_setup", skip_reason="phase2_waiting")

    if state.get("phase") == "phase3_live":
        active = active_trades_checker(
            strategy=state["strategy"],
            version=state["version"],
            setup_id=state["setup_id"],
            log_label=state["strategy"],
        )
        if not active.get("success"):
            _direct_state_log("phase3_db_error", state, error=active.get("error"))
            return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="live", skip_reason="phase3_db_error")

        bridge_rows = _bridge_setup_rows(state, state.get("setup_id"))
        managing_trade1 = _bridge_rows_from_active(state, state.get("setup_id"), active.get("data") or [], trade_number=1)
        managing_trade2 = _bridge_rows_from_active(state, state.get("setup_id"), active.get("data") or [], trade_number=2)
        avg_entry, avg_qty = _calc_avg_entry_from_rows(managing_trade1 + managing_trade2)
        if state.get("setup_id") and avg_entry is not None and avg_qty > 0:
            state["bridge_avg_entry"][state["setup_id"]] = avg_entry
            state["bridge_avg_qty"][state["setup_id"]] = avg_qty

        if active.get("managing_present"):
            cp = bridge_rows[0].get("cp") if bridge_rows else None
            be_price = state["bridge_avg_entry"].get(state.get("setup_id"))
            if cp == "call":
                if managing_trade1:
                    swing_ts = recent_low_ts
                    swing_level = recent_low_price
                    swing_key = f"{state.get('setup_id')}:swing_update_trade1"
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
                        _patch_sl_rows(state, managing_trade1, swing_level, "swing_update_trade1")
                        if managing_trade2 and be_price is not None:
                            _patch_sl_rows(state, managing_trade2, be_price, "be_update_trade2")
                elif managing_trade2:
                    swing_ts = recent_low_ts
                    swing_level = recent_low_price
                    swing_key = f"{state.get('setup_id')}:swing_update_trade2_only"
                    prev_swing_ts = state["bridge_last_swing_ts"].get(swing_key)
                    if swing_ts and swing_level is not None and (prev_swing_ts is None or swing_ts > prev_swing_ts):
                        state["bridge_last_swing_ts"][swing_key] = swing_ts
                        _patch_sl_rows(state, managing_trade2, swing_level, "swing_update_trade2_only")
            elif cp == "put":
                if managing_trade1:
                    swing_ts = recent_high_ts
                    swing_level = recent_high_price
                    swing_key = f"{state.get('setup_id')}:swing_update_trade1"
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
                        _patch_sl_rows(state, managing_trade1, swing_level, "swing_update_trade1")
                        if managing_trade2 and be_price is not None:
                            _patch_sl_rows(state, managing_trade2, be_price, "be_update_trade2")
                elif managing_trade2:
                    swing_ts = recent_high_ts
                    swing_level = recent_high_price
                    swing_key = f"{state.get('setup_id')}:swing_update_trade2_only"
                    prev_swing_ts = state["bridge_last_swing_ts"].get(swing_key)
                    if swing_ts and swing_level is not None and (prev_swing_ts is None or swing_ts > prev_swing_ts):
                        state["bridge_last_swing_ts"][swing_key] = swing_ts
                        _patch_sl_rows(state, managing_trade2, swing_level, "swing_update_trade2_only")

            return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="live", skip_reason="")

        if active.get("waiting_present"):
            cleanup = active_cleanup(
                strategy=state["strategy"],
                version=state["version"],
                setup_id=state["setup_id"],
                log_label=state["strategy"],
            )
            if cleanup.get("success"):
                _clear_direct_setup(state)
                return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="idle", skip_reason="phase3_cleanup_done")
            return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="live", skip_reason="phase3_cleanup_failed")

        _clear_direct_setup(state)
        return _direct_snapshot(state, last_ts=last_ts, last_ts_et=last_ts_et, status="idle", skip_reason="phase3_complete")

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
