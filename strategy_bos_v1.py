import os
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


_BOS_STATE: Dict[Tuple[str, str], Dict[str, Any]] = {}


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
    print(f"[BOS_V1] env fallback used for {name}: {raw!r}")
    return default


def _safe_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    val = _safe_float(raw)
    if val is None:
        print(f"[BOS_V1] env fallback used for {name}: {raw!r}")
        return default
    return val


def _safe_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        print(f"[BOS_V1] env fallback used for {name}: {raw!r}")
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


def _latest_swing(swings: Dict[str, Any], swing_type: str, current_ts: Optional[datetime]) -> Optional[Dict[str, Any]]:
    swing_items = (swings or {}).get("swings") or []
    latest = None
    latest_dt = None
    for s in swing_items:
        if s.get("type") != swing_type:
            continue
        s_ts = s.get("ts")
        s_dt = _parse_ts(s_ts)
        if current_ts and s_dt and s_dt >= current_ts:
            continue
        if latest is None:
            latest = s
            latest_dt = s_dt
            continue
        if s_dt and (latest_dt is None or s_dt > latest_dt):
            latest = s
            latest_dt = s_dt
        elif latest_dt is None and s_ts and str(s_ts) > str(latest.get("ts") or ""):
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
        "initial_capital": _safe_float_env("BOS_INITIAL_CAPITAL", 100000.0),
        "shares_per_trade": _safe_int_env("BOS_SHARES_PER_TRADE", 100),
        "max_open_positions": _safe_int_env("BOS_MAX_OPEN_POSITIONS", 1),
    }


def _ensure_state(symbol: str, timeframe: str) -> Dict[str, Any]:
    key = (symbol, timeframe)
    if key not in _BOS_STATE:
        cfg = _default_cfg()
        _BOS_STATE[key] = {
            "config": cfg,
            "cash": float(cfg["initial_capital"]),
            "open_position": None,
            "pending_entry": None,
            "broken_swing_highs": set(),
            "trade_id_counter": 0,
            "signal_id_counter": 0,
            "completed_trades": [],
            "signals": [],
            "latest_snapshot": None,
        }
    return _BOS_STATE[key]


def _as_et_str(ts: Optional[str]) -> Optional[str]:
    return ts


def evaluate_bos_score_v1(
    symbol: str,
    timeframe: str,
    candles: List[Dict[str, Any]],
    swings: Dict[str, Any],
) -> Dict[str, Any]:
    state = _ensure_state(symbol, timeframe)
    cfg = state["config"]

    last_candle = candles[-1] if candles else {}
    last_ts_raw = last_candle.get("ts") or last_candle.get("timestamp")
    last_dt = _parse_ts(last_ts_raw)
    last_ts = _ts_to_iso(last_dt) or (str(last_ts_raw) if last_ts_raw is not None else None)

    open_px = _safe_float(last_candle.get("open"))
    high_px = _safe_float(last_candle.get("high"))
    low_px = _safe_float(last_candle.get("low"))
    close_px = _safe_float(last_candle.get("close"))

    recent_high = _latest_swing(swings, "swing_high", last_dt)
    recent_low = _latest_swing(swings, "swing_low", last_dt)

    recent_high_price = _safe_float((recent_high or {}).get("price"))
    recent_low_price = _safe_float((recent_low or {}).get("price"))
    recent_high_ts = (recent_high or {}).get("ts")
    recent_low_ts = (recent_low or {}).get("ts")

    mom_val = _safe_float(last_candle.get("mom_atr"), 0.0) or 0.0
    vol_val = _safe_float(last_candle.get("vol_rel"), 0.0) or 0.0

    candle_range = None
    if high_px is not None and low_px is not None:
        candle_range = high_px - low_px

    close_strength_val = 0.0
    if candle_range and candle_range > 0 and close_px is not None and low_px is not None:
        close_strength_val = (close_px - low_px) / candle_range

    break_distance_val = 0.0
    if recent_high_price and recent_high_price > 0 and close_px is not None:
        break_distance_val = (close_px - recent_high_price) / recent_high_price

    momentum_pass = mom_val >= cfg["mom_threshold"]
    volume_pass = vol_val >= cfg["vol_threshold"]
    close_pass = bool(candle_range and candle_range > 0 and close_strength_val >= cfg["close_threshold"])
    break_pass = bool(recent_high_price and recent_high_price > 0 and break_distance_val >= cfg["break_threshold"])

    score_total = 0.0
    score_total += cfg["weight_momentum"] if momentum_pass else 0.0
    score_total += cfg["weight_volume"] if volume_pass else 0.0
    score_total += cfg["weight_close"] if close_pass else 0.0
    score_total += cfg["weight_break"] if break_pass else 0.0

    already_broken = False
    bos_detected = False
    skip_reason = ""
    swing_high_key = None
    if recent_high_price is None or recent_high_ts is None:
        skip_reason = "no_valid_recent_swing_high"
    else:
        swing_high_key = f"{recent_high_ts}|{recent_high_price}"
        already_broken = swing_high_key in state["broken_swing_highs"]
        if already_broken:
            skip_reason = "swing_high_already_broken"
        elif close_px is not None and close_px > recent_high_price:
            bos_detected = True
        else:
            skip_reason = "bos_not_detected"

    if recent_low_price is None or recent_low_ts is None:
        if not skip_reason:
            skip_reason = "no_valid_recent_swing_low"

    # 1) Fill pending at current candle open
    if state["pending_entry"] and open_px is not None:
        p = state["pending_entry"]
        shares = _safe_int(p.get("shares"), cfg["shares_per_trade"])
        position_cost = shares * open_px
        if position_cost <= state["cash"]:
            state["open_position"] = {
                "trade_id": p["trade_id"],
                "entry_ts": last_ts,
                "entry_ts_et": _as_et_str(last_ts),
                "entry_price": open_px,
                "entry_reason": "bos_up_score_pass",
                "entry_ref_swing_high": p.get("entry_ref_swing_high"),
                "entry_ref_swing_high_ts": p.get("entry_ref_swing_high_ts"),
                "entry_ref_swing_low": p.get("entry_ref_swing_low"),
                "entry_ref_swing_low_ts": p.get("entry_ref_swing_low_ts"),
                "bos_score_total": p.get("score_total", 0.0),
                "bos_score_threshold": cfg["score_min"],
                "bos_momentum_pass": p.get("momentum_pass", False),
                "bos_volume_pass": p.get("volume_pass", False),
                "bos_close_pass": p.get("close_pass", False),
                "bos_break_pass": p.get("break_pass", False),
                "bos_momentum_value": p.get("mom_value", 0.0),
                "bos_volume_value": p.get("vol_value", 0.0),
                "bos_close_strength_value": p.get("close_strength_value", 0.0),
                "bos_break_distance_value": p.get("break_distance_value", 0.0),
                "shares": shares,
                "position_cost": position_cost,
                "cash_before_entry": state["cash"],
                "cash_after_entry": state["cash"] - position_cost,
                "highest_price_during_trade": high_px if high_px is not None else open_px,
                "lowest_price_during_trade": low_px if low_px is not None else open_px,
                "bars_held": 0,
                "signal_ts": p.get("signal_ts"),
                "signal_ts_et": p.get("signal_ts_et"),
                "side": "long",
                "notes": "",
            }
            state["cash"] -= position_cost
            state["pending_entry"] = None
            print(f"[BOS_V1] pending entry filled {symbol} {timeframe} @ {open_px}")
        else:
            state["pending_entry"] = None
            print(f"[BOS_V1] signal skipped with reason insufficient_cash {symbol} {timeframe}")

    # 2) Update open position intratrade stats
    if state["open_position"]:
        pos = state["open_position"]
        if high_px is not None:
            pos["highest_price_during_trade"] = max(pos.get("highest_price_during_trade", high_px), high_px)
        if low_px is not None:
            pos["lowest_price_during_trade"] = min(pos.get("lowest_price_during_trade", low_px), low_px)
        pos["bars_held"] = _safe_int(pos.get("bars_held"), 0) + 1

    # 3) Exit check at close
    if state["open_position"] and close_px is not None and recent_low_price is not None and close_px < recent_low_price:
        pos = state["open_position"]
        shares = _safe_int(pos.get("shares"), cfg["shares_per_trade"])
        proceeds = shares * close_px
        state["cash"] += proceeds

        entry_price = _safe_float(pos.get("entry_price"), 0.0) or 0.0
        gross_pnl = (close_px - entry_price) * shares
        gross_pnl_pct = ((close_px - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0

        entry_dt = _parse_ts(pos.get("entry_ts"))
        exit_dt = _parse_ts(last_ts)
        holding_minutes = 0
        if entry_dt and exit_dt:
            holding_minutes = int((exit_dt - entry_dt).total_seconds() // 60)

        lowest = _safe_float(pos.get("lowest_price_during_trade"), close_px) or close_px
        highest = _safe_float(pos.get("highest_price_during_trade"), close_px) or close_px

        trade = {
            "trade_id": pos.get("trade_id"),
            "symbol": symbol,
            "timeframe": timeframe,
            "side": "long",
            "entry_ts": pos.get("entry_ts"),
            "entry_ts_et": pos.get("entry_ts_et"),
            "entry_price": entry_price,
            "entry_reason": "bos_up_score_pass",
            "entry_ref_swing_high": pos.get("entry_ref_swing_high"),
            "entry_ref_swing_high_ts": pos.get("entry_ref_swing_high_ts"),
            "entry_ref_swing_low": pos.get("entry_ref_swing_low"),
            "entry_ref_swing_low_ts": pos.get("entry_ref_swing_low_ts"),
            "bos_score_total": pos.get("bos_score_total"),
            "bos_score_threshold": pos.get("bos_score_threshold"),
            "bos_momentum_pass": pos.get("bos_momentum_pass"),
            "bos_volume_pass": pos.get("bos_volume_pass"),
            "bos_close_pass": pos.get("bos_close_pass"),
            "bos_break_pass": pos.get("bos_break_pass"),
            "bos_momentum_value": pos.get("bos_momentum_value"),
            "bos_volume_value": pos.get("bos_volume_value"),
            "bos_close_strength_value": pos.get("bos_close_strength_value"),
            "bos_break_distance_value": pos.get("bos_break_distance_value"),
            "shares": shares,
            "position_cost": pos.get("position_cost"),
            "cash_before_entry": pos.get("cash_before_entry"),
            "cash_after_entry": pos.get("cash_after_entry"),
            "exit_ts": last_ts,
            "exit_ts_et": _as_et_str(last_ts),
            "exit_price": close_px,
            "exit_reason": "close_below_recent_swing_low",
            "exit_ref_swing_low": recent_low_price,
            "exit_ref_swing_low_ts": recent_low_ts,
            "gross_pnl": gross_pnl,
            "gross_pnl_pct": gross_pnl_pct,
            "bars_held": pos.get("bars_held", 0),
            "holding_minutes": holding_minutes,
            "mae": entry_price - lowest,
            "mfe": highest - entry_price,
            "lowest_price_during_trade": lowest,
            "highest_price_during_trade": highest,
            "status": "closed",
            "notes": pos.get("notes", ""),
        }
        state["completed_trades"].append(trade)
        state["open_position"] = None
        print(f"[BOS_V1] exit triggered {symbol} {timeframe} @ {close_px}")

    # 4) Entry signal generation
    score_pass = score_total >= cfg["score_min"]
    if not cfg["enabled"]:
        skip_reason = "bos_score_disabled"

    if state["open_position"]:
        skip_reason = "position_already_open"
    elif state["pending_entry"]:
        skip_reason = "pending_entry_exists"

    if bos_detected and swing_high_key:
        if swing_high_key in state["broken_swing_highs"]:
            print(f"[BOS_V1] repeated broken swing ignored {symbol} {timeframe}")
        else:
            state["broken_swing_highs"].add(swing_high_key)
            print(f"[BOS_V1] BOS detected {symbol} {timeframe} break={recent_high_price}")

    if bos_detected and cfg["enabled"] and score_pass and not state["open_position"] and not state["pending_entry"]:
        needed_cash = cfg["shares_per_trade"] * (open_px if open_px is not None else (close_px or 0.0))
        if state["cash"] >= needed_cash:
            state["trade_id_counter"] += 1
            state["pending_entry"] = {
                "trade_id": state["trade_id_counter"],
                "signal_ts": last_ts,
                "signal_ts_et": _as_et_str(last_ts),
                "shares": cfg["shares_per_trade"],
                "entry_ref_swing_high": recent_high_price,
                "entry_ref_swing_high_ts": recent_high_ts,
                "entry_ref_swing_low": recent_low_price,
                "entry_ref_swing_low_ts": recent_low_ts,
                "score_total": score_total,
                "momentum_pass": momentum_pass,
                "volume_pass": volume_pass,
                "close_pass": close_pass,
                "break_pass": break_pass,
                "mom_value": mom_val,
                "vol_value": vol_val,
                "close_strength_value": close_strength_val,
                "break_distance_value": break_distance_val,
            }
            print(f"[BOS_V1] pending entry created {symbol} {timeframe}")
            skip_reason = ""
        else:
            skip_reason = "insufficient_cash"
            print(f"[BOS_V1] signal skipped with reason insufficient_cash {symbol} {timeframe}")
    elif bos_detected and not score_pass and not skip_reason:
        skip_reason = "score_below_threshold"

    state["signal_id_counter"] += 1
    signal = {
        "signal_id": state["signal_id_counter"],
        "ts": last_ts,
        "ts_et": _as_et_str(last_ts),
        "symbol": symbol,
        "timeframe": timeframe,
        "signal_type": "bos_up_check",
        "recent_swing_high": recent_high_price,
        "recent_swing_high_ts": recent_high_ts,
        "recent_swing_low": recent_low_price,
        "recent_swing_low_ts": recent_low_ts,
        "open": open_px,
        "high": high_px,
        "low": low_px,
        "close": close_px,
        "bos_detected": bos_detected,
        "already_broken": already_broken,
        "score_total": score_total,
        "score_threshold": cfg["score_min"],
        "score_pass": score_pass,
        "mom_pass": momentum_pass,
        "vol_pass": volume_pass,
        "close_pass": close_pass,
        "break_pass": break_pass,
        "mom_value": mom_val,
        "vol_value": vol_val,
        "close_strength_value": close_strength_val,
        "break_distance_value": break_distance_val,
        "skip_reason": skip_reason,
        "notes": "",
    }
    state["signals"].append(signal)

    snapshot = {
        "id": "bos_score_v1",
        "symbol": symbol,
        "timeframe": timeframe,
        "last_eval_ts": last_ts,
        "last_eval_ts_et": _as_et_str(last_ts),
        "status": "ok" if cfg["enabled"] else "disabled",
        "bos_detected": bos_detected,
        "recent_swing_high": recent_high_price,
        "recent_swing_high_ts": recent_high_ts,
        "recent_swing_low": recent_low_price,
        "recent_swing_low_ts": recent_low_ts,
        "score_enabled": cfg["enabled"],
        "score_total": score_total,
        "score_threshold": cfg["score_min"],
        "score_pass": score_pass,
        "momentum_pass": momentum_pass,
        "volume_pass": volume_pass,
        "close_pass": close_pass,
        "break_pass": break_pass,
        "momentum_value": mom_val,
        "volume_value": vol_val,
        "close_strength_value": close_strength_val,
        "break_distance_value": break_distance_val,
        "cash": state["cash"],
        "position_open": state["open_position"] is not None,
        "pending_entry": deepcopy(state["pending_entry"]),
        "shares_per_trade": cfg["shares_per_trade"],
        "initial_capital": cfg["initial_capital"],
        "max_open_positions": cfg["max_open_positions"],
        "last_signal": deepcopy(state["signals"][-1] if state["signals"] else None),
        "last_trade": deepcopy(state["completed_trades"][-1] if state["completed_trades"] else None),
        "trade_count": len(state["completed_trades"]),
        "signal_count": len(state["signals"]),
        "signals": state["signals"],
        "completed_trades": state["completed_trades"],
    }
    state["latest_snapshot"] = snapshot
    return snapshot
