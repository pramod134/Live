import os
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo


_BOS_STATE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_ET = ZoneInfo("America/New_York")


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


def _as_et_str(ts_value: Any) -> Optional[str]:
    dt = _parse_ts(ts_value)
    if dt is None:
        if isinstance(ts_value, str) and ts_value:
            return ts_value
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(_ET).isoformat()


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
            "broken_swing_lows": set(),
            "trade_id_counter": 0,
            "signal_id_counter": 0,
            "completed_trades": [],
            "signals": [],
            "latest_snapshot": None,
        }
    return _BOS_STATE[key]


def _trade_list_with_pl(trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for t in trades:
        pnl = _safe_float(t.get("gross_pnl"), 0.0) or 0.0
        out.append(
            {
                "trade_id": t.get("trade_id"),
                "symbol": t.get("symbol"),
                "timeframe": t.get("timeframe"),
                "side": t.get("side"),
                "entry_ts": t.get("entry_ts"),
                "break_candle_ts": t.get("break_candle_ts"),
                "break_candle_ts_et": t.get("break_candle_ts_et"),
                "reference_swing_ts": t.get("reference_swing_ts"),
                "reference_swing_score": t.get("reference_swing_score"),
                "bos_score_total": t.get("bos_score_total"),
                "bos_close_threshold": t.get("bos_close_threshold"),
                "bos_break_threshold": t.get("bos_break_threshold"),
                "bos_momentum_value": t.get("bos_momentum_value"),
                "bos_volume_value": t.get("bos_volume_value"),
                "bos_close_strength_value": t.get("bos_close_strength_value"),
                "bos_break_distance_value": t.get("bos_break_distance_value"),
                "structure_state_tf": t.get("structure_state_tf"),
                "structure_state_15m": t.get("structure_state_15m"),
                "structure_state_1h": t.get("structure_state_1h"),
                "exit_ts": t.get("exit_ts"),
                "exit_swing_reference_ts": t.get("exit_swing_reference_ts"),
                "entry_price": t.get("entry_price"),
                "exit_price": t.get("exit_price"),
                "shares": t.get("shares"),
                "gross_pnl": pnl,
                "result": "profit" if pnl > 0 else "loss" if pnl < 0 else "flat",
                "exit_reason": t.get("exit_reason"),
            }
        )
    return out


def _trade_summary(state: Dict[str, Any], symbol: Optional[str] = None, timeframe: Optional[str] = None) -> Dict[str, Any]:
    trades = state["completed_trades"]
    total_profit_loss = sum((_safe_float(t.get("gross_pnl"), 0.0) or 0.0) for t in trades)
    wins = sum(1 for t in trades if (_safe_float(t.get("gross_pnl"), 0.0) or 0.0) > 0)
    losses = sum(1 for t in trades if (_safe_float(t.get("gross_pnl"), 0.0) or 0.0) < 0)
    flats = len(trades) - wins - losses
    avg_profit_loss_per_trade = (total_profit_loss / len(trades)) if trades else 0.0
    long_trades = [t for t in trades if str(t.get("side") or "").lower() == "long"]
    short_trades = [t for t in trades if str(t.get("side") or "").lower() == "short"]

    def _side_summary(side_trades: List[Dict[str, Any]], side: str) -> Dict[str, Any]:
        side_total_pl = sum((_safe_float(t.get("gross_pnl"), 0.0) or 0.0) for t in side_trades)
        return {
            "side": side,
            "timeframe": timeframe,
            "total_trades": len(side_trades),
            "total_profit_loss": side_total_pl,
        }

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "initial_capital": state["config"]["initial_capital"],
        "current_cash": state["cash"],
        "total_trades": len(trades),
        "winning_trades": wins,
        "losing_trades": losses,
        "flat_trades": flats,
        "total_profit_loss": total_profit_loss,
        "avg_profit_loss_per_trade": avg_profit_loss_per_trade,
        "long_summary": _side_summary(long_trades, "long"),
        "short_summary": _side_summary(short_trades, "short"),
        "trade_list": _trade_list_with_pl(trades),
    }


def print_bos_final_summaries() -> None:
    print("[BOS_V1] FINAL SUMMARY START")
    for (symbol, timeframe), state in sorted(_BOS_STATE.items()):
        cfg = state["config"]
        summary = _trade_summary(state, symbol, timeframe)
        print(
            f"[BOS_V1] FINAL SUMMARY | Symbol={symbol} | TF={timeframe} | "
            f"Initial={cfg['initial_capital']:.2f} | Cash={state['cash']:.2f} | "
            f"Trades={summary['total_trades']} | Wins={summary['winning_trades']} | "
            f"Losses={summary['losing_trades']} | Flat={summary['flat_trades']} | "
            f"TotalPnL={summary['total_profit_loss']:.2f} | "
            f"AvgPnL={summary['avg_profit_loss_per_trade']:.2f}"
        )
        for side_key in ("long_summary", "short_summary"):
            side_summary = summary.get(side_key) or {}
            print(
                f"[BOS_V1] FINAL SIDE SUMMARY | Symbol={symbol} | TF={timeframe} | "
                f"Side={side_summary.get('side')} | "
                f"TotalTrades={int(side_summary.get('total_trades', 0) or 0)} | "
                f"TotalPnL={float(side_summary.get('total_profit_loss', 0.0) or 0.0):.2f}"
            )
        for t in summary["trade_list"]:
            print(
                f"[BOS_V1] FINAL TRADE | Symbol={symbol} | TF={timeframe} | "
                f"TradeID={t['trade_id']} | Side={t.get('side')} | Entry={t['entry_price']} | Exit={t['exit_price']} | "
                f"BreakTS={t.get('break_candle_ts')} | RefSwingTS={t.get('reference_swing_ts')} | "
                f"RefSwingScore={t.get('reference_swing_score')} | "
                f"BOSScore={t.get('bos_score_total')} | "
                f"MomVal={t.get('bos_momentum_value')} | "
                f"VolVal={t.get('bos_volume_value')} | "
                f"CloseStrength={t.get('bos_close_strength_value')} | "
                f"BreakDistance={t.get('bos_break_distance_value')} | "
                f"StructTF={t.get('structure_state_tf')} | "
                f"Struct15m={t.get('structure_state_15m')} | "
                f"Struct1h={t.get('structure_state_1h')} | "
                f"ExitTS={t.get('exit_ts')} | ExitSwingRefTS={t.get('exit_swing_reference_ts')} | "
                f"PnL={t['gross_pnl']:.2f} | Result={t['result']}"
            )
    print("[BOS_V1] FINAL SUMMARY END")


def evaluate_bos_score_v1(
    symbol: str,
    timeframe: str,
    candles: List[Dict[str, Any]],
    swings: Dict[str, Any],
    structure_state_tf: Optional[str] = None,
    structure_state_15m: Optional[str] = None,
    structure_state_1h: Optional[str] = None,
) -> Dict[str, Any]:
    state = _ensure_state(symbol, timeframe)
    cfg = state["config"]

    last_candle = candles[-1] if candles else {}
    last_ts_raw = last_candle.get("ts") or last_candle.get("timestamp")
    last_dt = _parse_ts(last_ts_raw)
    last_ts = _ts_to_iso(last_dt) or (str(last_ts_raw) if last_ts_raw is not None else None)
    last_ts_et = last_candle.get("ts_et") or _as_et_str(last_ts)

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
    recent_high_pivot_score = _safe_float(((recent_high or {}).get("pivot") or {}).get("score"))
    recent_low_pivot_score = _safe_float(((recent_low or {}).get("pivot") or {}).get("score"))

    mom_val = _safe_float(last_candle.get("mom_atr"), 0.0) or 0.0
    vol_val = _safe_float(last_candle.get("vol_rel"), 0.0) or 0.0
    structure_state_tf_val = str(structure_state_tf) if structure_state_tf is not None else None
    structure_state_15m_val = str(structure_state_15m) if structure_state_15m is not None else None
    structure_state_1h_val = str(structure_state_1h) if structure_state_1h is not None else None

    candle_range = None
    if high_px is not None and low_px is not None:
        candle_range = high_px - low_px

    close_strength_long = 0.0
    if candle_range is not None and candle_range > 0 and close_px is not None and low_px is not None:
        close_strength_long = (close_px - low_px) / candle_range

    close_strength_short = 0.0
    if candle_range is not None and candle_range > 0 and close_px is not None and high_px is not None:
        close_strength_short = (high_px - close_px) / candle_range

    break_distance_long = 0.0
    if recent_high_price is not None and recent_high_price > 0 and close_px is not None:
        break_distance_long = (close_px - recent_high_price) / recent_high_price

    break_distance_short = 0.0
    if recent_low_price is not None and recent_low_price > 0 and close_px is not None:
        break_distance_short = (recent_low_price - close_px) / recent_low_price

    momentum_pass_long = mom_val >= cfg["mom_threshold"]
    volume_pass_long = vol_val >= cfg["vol_threshold"]
    close_pass_long = bool(candle_range is not None and candle_range > 0 and close_strength_long >= cfg["close_threshold"])
    break_pass_long = bool(recent_high_price is not None and recent_high_price > 0 and break_distance_long >= cfg["break_threshold"])

    momentum_pass_short = mom_val >= cfg["mom_threshold"]
    volume_pass_short = vol_val >= cfg["vol_threshold"]
    close_pass_short = bool(candle_range is not None and candle_range > 0 and close_strength_short >= cfg["close_threshold"])
    break_pass_short = bool(recent_low_price is not None and recent_low_price > 0 and break_distance_short >= cfg["break_threshold"])

    score_total_long = 0.0
    score_total_long += cfg["weight_momentum"] if momentum_pass_long else 0.0
    score_total_long += cfg["weight_volume"] if volume_pass_long else 0.0
    score_total_long += cfg["weight_close"] if close_pass_long else 0.0
    score_total_long += cfg["weight_break"] if break_pass_long else 0.0
    score_pass_long = score_total_long >= cfg["score_min"]

    score_total_short = 0.0
    score_total_short += cfg["weight_momentum"] if momentum_pass_short else 0.0
    score_total_short += cfg["weight_volume"] if volume_pass_short else 0.0
    score_total_short += cfg["weight_close"] if close_pass_short else 0.0
    score_total_short += cfg["weight_break"] if break_pass_short else 0.0
    score_pass_short = score_total_short >= cfg["score_min"]

    long_already_broken = False
    short_already_broken = False
    long_bos_detected = False
    short_bos_detected = False
    skip_reason = ""
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

    status = "idle"

    # 1) Fill pending entry at current candle open.
    if state["pending_entry"] and open_px is not None:
        pending = state["pending_entry"]
        side = pending.get("side", "long")
        shares = _safe_int(pending.get("shares"), cfg["shares_per_trade"])
        position_cost = shares * open_px
        if position_cost <= state["cash"]:
            state["open_position"] = {
                "trade_id": pending["trade_id"],
                "entry_ts": last_ts,
                "entry_ts_et": last_ts_et,
                "entry_price": open_px,
                "entry_reason": "bos_up_score_pass" if side == "long" else "bos_down_score_pass",
                "entry_ref_swing_high": pending.get("entry_ref_swing_high"),
                "entry_ref_swing_high_ts": pending.get("entry_ref_swing_high_ts"),
                "entry_ref_swing_high_score": pending.get("entry_ref_swing_high_score"),
                "entry_ref_swing_low": pending.get("entry_ref_swing_low"),
                "entry_ref_swing_low_ts": pending.get("entry_ref_swing_low_ts"),
                "entry_ref_swing_low_score": pending.get("entry_ref_swing_low_score"),
                "entry_bos_type": "bos_up" if side == "long" else "bos_down",
                "entry_bos_level": pending.get("entry_ref_swing_high") if side == "long" else pending.get("entry_ref_swing_low"),
                "entry_bos_level_ts": pending.get("entry_ref_swing_high_ts") if side == "long" else pending.get("entry_ref_swing_low_ts"),
                "bos_score_total": pending.get("score_total", 0.0),
                "bos_score_threshold": cfg["score_min"],
                "bos_score_pass": pending.get("score_pass", False),
                "bos_momentum_pass": pending.get("momentum_pass", False),
                "bos_volume_pass": pending.get("volume_pass", False),
                "bos_close_pass": pending.get("close_pass", False),
                "bos_break_pass": pending.get("break_pass", False),
                "bos_momentum_value": pending.get("mom_value", 0.0),
                "bos_volume_value": pending.get("vol_value", 0.0),
                "bos_close_strength_value": pending.get("close_strength_value", 0.0),
                "bos_break_distance_value": pending.get("break_distance_value", 0.0),
                "bos_momentum_threshold": cfg["mom_threshold"],
                "bos_volume_threshold": cfg["vol_threshold"],
                "bos_close_threshold": cfg["close_threshold"],
                "bos_break_threshold": cfg["break_threshold"],
                "shares": shares,
                "position_cost": position_cost,
                "cash_before_entry": state["cash"],
                "cash_after_entry": state["cash"] - position_cost,
                "highest_price_during_trade": high_px if high_px is not None else open_px,
                "lowest_price_during_trade": low_px if low_px is not None else open_px,
                "bars_held": 0,
                "signal_ts": pending.get("signal_ts"),
                "signal_ts_et": pending.get("signal_ts_et"),
                "structure_state_tf": pending.get("structure_state_tf"),
                "structure_state_15m": pending.get("structure_state_15m"),
                "structure_state_1h": pending.get("structure_state_1h"),
                "side": side,
                "notes": pending.get("notes", ""),
            }
            state["cash"] -= position_cost
            state["pending_entry"] = None
            status = "in_position"
            print(f"[BOS_V1] pending {side} entry filled {symbol} {timeframe} @ {open_px}")
        else:
            state["pending_entry"] = None
            status = "skipped"
            print(f"[BOS_V1] signal skipped with reason insufficient_cash {symbol} {timeframe}")
    elif state["pending_entry"]:
        status = "pending_entry"

    # 2) Update open-position excursion stats.
    if state["open_position"]:
        pos = state["open_position"]
        if high_px is not None:
            pos["highest_price_during_trade"] = max(pos.get("highest_price_during_trade", high_px), high_px)
        if low_px is not None:
            pos["lowest_price_during_trade"] = min(pos.get("lowest_price_during_trade", low_px), low_px)
        pos["bars_held"] = _safe_int(pos.get("bars_held"), 0) + 1
        status = "in_position"

    # 3) Exit check at close.
    exit_long = (
        state["open_position"]
        and state["open_position"].get("side", "long") == "long"
        and close_px is not None
        and recent_low_price is not None
        and close_px < recent_low_price
    )
    exit_short = (
        state["open_position"]
        and state["open_position"].get("side", "long") == "short"
        and close_px is not None
        and recent_high_price is not None
        and close_px > recent_high_price
    )
    if exit_long or exit_short:
        pos = state["open_position"]
        pos_side = pos.get("side", "long")
        shares = _safe_int(pos.get("shares"), cfg["shares_per_trade"])
        proceeds = shares * close_px
        state["cash"] += proceeds

        entry_price = _safe_float(pos.get("entry_price"), 0.0) or 0.0
        if pos_side == "short":
            gross_pnl = (entry_price - close_px) * shares
            gross_pnl_pct = ((entry_price - close_px) / entry_price) if entry_price > 0 else 0.0
        else:
            gross_pnl = (close_px - entry_price) * shares
            gross_pnl_pct = ((close_px - entry_price) / entry_price) if entry_price > 0 else 0.0

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
            "side": pos_side,
            "status": "closed",
            "entry_ts": pos.get("entry_ts"),
            "entry_ts_et": pos.get("entry_ts_et"),
            "break_candle_ts": pos.get("signal_ts"),
            "break_candle_ts_et": pos.get("signal_ts_et"),
            "reference_swing_ts": pos.get("entry_ref_swing_high_ts") if pos_side == "long" else pos.get("entry_ref_swing_low_ts"),
            "reference_swing_score": pos.get("entry_ref_swing_high_score") if pos_side == "long" else pos.get("entry_ref_swing_low_score"),
            "entry_price": entry_price,
            "entry_reason": "bos_up_score_pass" if pos_side == "long" else "bos_down_score_pass",
            "entry_ref_swing_high": pos.get("entry_ref_swing_high"),
            "entry_ref_swing_high_ts": pos.get("entry_ref_swing_high_ts"),
            "entry_ref_swing_high_score": pos.get("entry_ref_swing_high_score"),
            "entry_ref_swing_low": pos.get("entry_ref_swing_low"),
            "entry_ref_swing_low_ts": pos.get("entry_ref_swing_low_ts"),
            "entry_ref_swing_low_score": pos.get("entry_ref_swing_low_score"),
            "entry_bos_type": pos.get("entry_bos_type"),
            "entry_bos_level": pos.get("entry_bos_level"),
            "entry_bos_level_ts": pos.get("entry_bos_level_ts"),
            "bos_score_total": pos.get("bos_score_total"),
            "bos_score_threshold": pos.get("bos_score_threshold"),
            "bos_score_pass": pos.get("bos_score_pass"),
            "bos_momentum_pass": pos.get("bos_momentum_pass"),
            "bos_volume_pass": pos.get("bos_volume_pass"),
            "bos_close_pass": pos.get("bos_close_pass"),
            "bos_break_pass": pos.get("bos_break_pass"),
            "bos_momentum_value": pos.get("bos_momentum_value"),
            "bos_volume_value": pos.get("bos_volume_value"),
            "bos_close_strength_value": pos.get("bos_close_strength_value"),
            "bos_break_distance_value": pos.get("bos_break_distance_value"),
            "bos_momentum_threshold": pos.get("bos_momentum_threshold"),
            "bos_volume_threshold": pos.get("bos_volume_threshold"),
            "bos_close_threshold": pos.get("bos_close_threshold"),
            "bos_break_threshold": pos.get("bos_break_threshold"),
            "structure_state_tf": pos.get("structure_state_tf"),
            "structure_state_15m": pos.get("structure_state_15m"),
            "structure_state_1h": pos.get("structure_state_1h"),
            "shares": shares,
            "position_cost": pos.get("position_cost"),
            "cash_before_entry": pos.get("cash_before_entry"),
            "cash_after_entry": pos.get("cash_after_entry"),
            "exit_ts": last_ts,
            "exit_ts_et": last_ts_et,
            "exit_price": close_px,
            "exit_reason": "close_below_recent_swing_low" if pos_side == "long" else "close_above_recent_swing_high",
            "exit_ref_swing_low": recent_low_price,
            "exit_ref_swing_low_ts": recent_low_ts,
            "exit_ref_swing_high": recent_high_price,
            "exit_ref_swing_high_ts": recent_high_ts,
            "exit_swing_reference_ts": recent_low_ts if pos_side == "long" else recent_high_ts,
            "gross_pnl": gross_pnl,
            "gross_pnl_pct": gross_pnl_pct,
            "bars_held": pos.get("bars_held", 0),
            "holding_minutes": holding_minutes,
            "mae": (entry_price - lowest) if pos_side == "long" else (highest - entry_price),
            "mfe": (highest - entry_price) if pos_side == "long" else (entry_price - lowest),
            "lowest_price_during_trade": lowest,
            "highest_price_during_trade": highest,
            "notes": pos.get("notes", ""),
        }
        state["completed_trades"].append(trade)
        state["open_position"] = None
        status = "exited"
        print(f"[BOS_V1] exit triggered {pos_side.upper()} {symbol} {timeframe} @ {close_px}")
        # End-of-run reporting is handled by print_bos_final_summaries().

    # 4) Entry signal generation.
    if not cfg["enabled"]:
        skip_reason = "bos_score_disabled"
        status = "disabled"

    if state["open_position"]:
        skip_reason = "position_already_open"
        if status not in {"exited"}:
            status = "in_position"
    elif state["pending_entry"]:
        skip_reason = "pending_entry_exists"
        if status not in {"exited"}:
            status = "pending_entry"

    if long_bos_detected and swing_high_key:
        if swing_high_key in state["broken_swing_highs"]:
            print(f"[BOS_V1] repeated broken swing high ignored {symbol} {timeframe}")
        else:
            state["broken_swing_highs"].add(swing_high_key)
            print(f"[BOS_V1] BOS detected LONG {symbol} {timeframe} break={recent_high_price}")

    if short_bos_detected and swing_low_key:
        if swing_low_key in state["broken_swing_lows"]:
            print(f"[BOS_V1] repeated broken swing low ignored {symbol} {timeframe}")
        else:
            state["broken_swing_lows"].add(swing_low_key)
            print(f"[BOS_V1] BOS detected SHORT {symbol} {timeframe} break={recent_low_price}")

    chosen_side = "none"
    chosen_score_total = 0.0
    chosen_score_pass = False
    chosen_momentum_pass = False
    chosen_volume_pass = False
    chosen_close_pass = False
    chosen_break_pass = False
    chosen_close_strength = 0.0
    chosen_break_distance = 0.0
    chosen_bos_detected = False
    chosen_signal_type = "none"

    if long_bos_detected and short_bos_detected:
        if score_total_long > score_total_short:
            chosen_side = "long"
        elif score_total_short > score_total_long:
            chosen_side = "short"
        else:
            skip_reason = "conflicting_long_short_signal"
    elif long_bos_detected:
        chosen_side = "long"
    elif short_bos_detected:
        chosen_side = "short"
    elif not skip_reason:
        if recent_high_price is None or recent_high_ts is None:
            skip_reason = "no_valid_recent_swing_high"
        elif recent_low_price is None or recent_low_ts is None:
            skip_reason = "no_valid_recent_swing_low"
        else:
            skip_reason = "bos_not_detected"

    if chosen_side == "long":
        chosen_score_total = score_total_long
        chosen_score_pass = score_pass_long
        chosen_momentum_pass = momentum_pass_long
        chosen_volume_pass = volume_pass_long
        chosen_close_pass = close_pass_long
        chosen_break_pass = break_pass_long
        chosen_close_strength = close_strength_long
        chosen_break_distance = break_distance_long
        chosen_bos_detected = long_bos_detected
        chosen_signal_type = "bos_up_check"
    elif chosen_side == "short":
        chosen_score_total = score_total_short
        chosen_score_pass = score_pass_short
        chosen_momentum_pass = momentum_pass_short
        chosen_volume_pass = volume_pass_short
        chosen_close_pass = close_pass_short
        chosen_break_pass = break_pass_short
        chosen_close_strength = close_strength_short
        chosen_break_distance = break_distance_short
        chosen_bos_detected = short_bos_detected
        chosen_signal_type = "bos_down_check"

    if (
        chosen_bos_detected
        and cfg["enabled"]
        and chosen_score_pass
        and not state["open_position"]
        and not state["pending_entry"]
        and cfg["max_open_positions"] >= 1
    ):
        needed_cash = cfg["shares_per_trade"] * (open_px if open_px is not None else (close_px or 0.0))
        if state["cash"] >= needed_cash:
            state["trade_id_counter"] += 1
            trade_id = f"{symbol}_{timeframe}_{state['trade_id_counter']}"
            state["pending_entry"] = {
                "trade_id": trade_id,
                "side": chosen_side,
                "signal_ts": last_ts,
                "signal_ts_et": last_ts_et,
                "shares": cfg["shares_per_trade"],
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
                "structure_state_tf": structure_state_tf_val,
                "structure_state_15m": structure_state_15m_val,
                "structure_state_1h": structure_state_1h_val,
                "notes": "first_break_of_unbroken_swing_high" if chosen_side == "long" else "first_break_of_unbroken_swing_low",
            }
            status = "signal_detected"
            skip_reason = ""
            print(f"[BOS_V1] pending {chosen_side} entry created {symbol} {timeframe}")
        else:
            skip_reason = "insufficient_cash"
            status = "skipped"
            print(f"[BOS_V1] signal skipped with reason insufficient_cash {symbol} {timeframe}")
    elif chosen_bos_detected and not chosen_score_pass and not skip_reason:
        skip_reason = "score_below_threshold"
        status = "skipped"
    elif status == "idle" and skip_reason:
        status = "skipped" if skip_reason != "bos_not_detected" else "idle"

    state["signal_id_counter"] += 1
    signal = {
        "signal_id": state["signal_id_counter"],
        "ts": last_ts,
        "ts_et": last_ts_et,
        "symbol": symbol,
        "timeframe": timeframe,
        "signal_type": chosen_signal_type,
        "signal_side": chosen_side,
        "recent_swing_high": recent_high_price,
        "recent_swing_high_ts": recent_high_ts,
        "recent_swing_low": recent_low_price,
        "recent_swing_low_ts": recent_low_ts,
        "open": open_px,
        "high": high_px,
        "low": low_px,
        "close": close_px,
        "bos_detected": chosen_bos_detected,
        "long_bos_detected": long_bos_detected,
        "short_bos_detected": short_bos_detected,
        "chosen_side": chosen_side,
        "already_broken": long_already_broken if chosen_side == "long" else short_already_broken if chosen_side == "short" else False,
        "score_total": chosen_score_total,
        "score_threshold": cfg["score_min"],
        "score_pass": chosen_score_pass,
        "mom_pass": chosen_momentum_pass,
        "vol_pass": chosen_volume_pass,
        "close_pass": chosen_close_pass,
        "break_pass": chosen_break_pass,
        "mom_value": mom_val,
        "vol_value": vol_val,
        "close_strength_value": chosen_close_strength,
        "break_distance_value": chosen_break_distance,
        "structure_state_tf": structure_state_tf_val,
        "structure_state_15m": structure_state_15m_val,
        "structure_state_1h": structure_state_1h_val,
        "skip_reason": skip_reason,
        "notes": "",
    }
    state["signals"].append(signal)

    summary = _trade_summary(state, symbol, timeframe)
    snapshot = {
        "id": "bos_score_v1",
        "symbol": symbol,
        "timeframe": timeframe,
        "last_eval_ts": last_ts,
        "last_eval_ts_et": last_ts_et,
        "status": status,
        "bos_detected": chosen_bos_detected,
        "long_bos_detected": long_bos_detected,
        "short_bos_detected": short_bos_detected,
        "chosen_side": chosen_side,
        "recent_swing_high": recent_high_price,
        "recent_swing_high_ts": recent_high_ts,
        "recent_swing_low": recent_low_price,
        "recent_swing_low_ts": recent_low_ts,
        "score_enabled": cfg["enabled"],
        "score_total": chosen_score_total,
        "score_threshold": cfg["score_min"],
        "score_pass": chosen_score_pass,
        "momentum_pass": chosen_momentum_pass,
        "volume_pass": chosen_volume_pass,
        "close_pass": chosen_close_pass,
        "break_pass": chosen_break_pass,
        "momentum_value": mom_val,
        "volume_value": vol_val,
        "close_strength_value": chosen_close_strength,
        "break_distance_value": chosen_break_distance,
        "cash": state["cash"],
        "current_cash": state["cash"],
        "position_open": state["open_position"] is not None,
        "open_position_side": (state["open_position"] or {}).get("side"),
        "pending_entry_side": (state["pending_entry"] or {}).get("side"),
        "pending_entry": deepcopy(state["pending_entry"]),
        "shares_per_trade": cfg["shares_per_trade"],
        "initial_capital": cfg["initial_capital"],
        "max_open_positions": cfg["max_open_positions"],
        "last_signal": deepcopy(state["signals"][-1] if state["signals"] else None),
        "last_trade": deepcopy(state["completed_trades"][-1] if state["completed_trades"] else None),
        "trade_count": len(state["completed_trades"]),
        "signal_count": len(state["signals"]),
        "trade_summary": summary,
        "trade_list": summary["trade_list"],
        "total_profit_loss": summary["total_profit_loss"],
        "avg_profit_loss_per_trade": summary["avg_profit_loss_per_trade"],
        "open_position_summary": deepcopy(state["open_position"]),
    }
    state["latest_snapshot"] = snapshot
    return snapshot
