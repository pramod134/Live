import os
import json
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo


_BOS_FVG_STATE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_ET = ZoneInfo("America/New_York")
DEBUG_LOGS = str(os.getenv("BOS_FVG_DEBUG_LOGS", "0")).strip().lower() in {"1", "true", "t", "yes", "y", "on"}
ENTRY_LEG_SHARES = 100


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
        print(f"[BOS_FVG_V1] env fallback used for {name}: {raw!r}")
    return default


def _safe_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    val = _safe_float(raw)
    if val is None:
        if DEBUG_LOGS:
            print(f"[BOS_FVG_V1] env fallback used for {name}: {raw!r}")
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
            print(f"[BOS_FVG_V1] env fallback used for {name}: {raw!r}")
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
        "initial_capital": _safe_float_env("BOS_INITIAL_CAPITAL", 300000.0),
        "shares_per_trade": _safe_int_env("BOS_SHARES_PER_TRADE", 100),
        "max_open_positions": _safe_int_env("BOS_MAX_OPEN_POSITIONS", 1),
    }


def _ensure_state(symbol: str, timeframe: str) -> Dict[str, Any]:
    key = (symbol, timeframe)
    if key not in _BOS_FVG_STATE:
        cfg = _default_cfg()
        _BOS_FVG_STATE[key] = {
            "config": cfg,
            "cash": float(cfg["initial_capital"]),
            "open_position": None,
            "pending_setup": None,
            "pending_entry": None,
            "broken_swing_highs": set(),
            "broken_swing_lows": set(),
            "trade_id_counter": 0,
            "signal_id_counter": 0,
            "completed_trades": [],
            "signals": [],
            "latest_snapshot": None,
            "final_logs_emitted": False,
        }
    return _BOS_FVG_STATE[key]


def _log_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    return str(value)


def _trade_ref_fields(trade: Dict[str, Any]) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    side = str(trade.get("side") or "").lower()
    if side == "long":
        return trade.get("entry_ref_swing_high_ts"), _safe_float(trade.get("entry_ref_swing_high")), _safe_float(trade.get("entry_ref_swing_high_score"))
    return trade.get("entry_ref_swing_low_ts"), _safe_float(trade.get("entry_ref_swing_low")), _safe_float(trade.get("entry_ref_swing_low_score"))


def _build_final_trade_log(trade: Dict[str, Any], symbol: str, timeframe: str) -> str:
    ref_ts, ref_price, ref_score = _trade_ref_fields(trade)
    entry_top_filled = bool(trade.get("entry_top_filled", False))
    entry_bottom_filled = bool(trade.get("entry_bottom_filled", False))
    partial_exit_count = _safe_int(trade.get("partial_exit_count"), 0)
    bos_exit_count = _safe_int(trade.get("bos_exit_count"), 0)
    return (
        f"[BOS_FVG_V1] FINAL TRADE | Symbol={symbol} | TF={timeframe} | "
        f"TradeID={_log_value(trade.get('trade_id'))} | Side={_log_value(trade.get('side'))} | "
        f"BOS_TS={_log_value(trade.get('bos_ts'))} | BOSDir={_log_value(trade.get('side'))} | "
        f"RefSwingTS={_log_value(ref_ts)} | RefSwingPrice={_log_value(ref_price)} | RefSwingScore={_log_value(ref_score)} | "
        f"BOSScore={_log_value(trade.get('bos_score_total'))} | MomVal={_log_value(trade.get('bos_momentum_value'))} | "
        f"VolVal={_log_value(trade.get('bos_volume_value'))} | CloseStrength={_log_value(trade.get('bos_close_strength_value'))} | "
        f"BreakDistance={_log_value(trade.get('bos_break_distance_value'))} | StructureStateTF={_log_value(trade.get('structure_state_tf'))} | "
        f"StructureState15m={_log_value(trade.get('structure_state_15m'))} | StructureState1h={_log_value(trade.get('structure_state_1h'))} | "
        f"FVGTS={_log_value(trade.get('fvg_ts'))} | FVGHigh={_log_value(trade.get('fvg_high'))} | FVGLow={_log_value(trade.get('fvg_low'))} | "
        f"FVGScore={_log_value(trade.get('fvg_score'))} | TradeScore={_log_value(trade.get('trade_score'))} | "
        f"FVGScoreSrc={_log_value(trade.get('fvg_score_src'))} | TradeScoreSrc={_log_value(trade.get('trade_score_src'))} | "
        f"FVGAfterBOS=true | EntryType={_log_value(trade.get('entry_type'))} | EntryTopPrice={_log_value(trade.get('entry_top_price'))} | "
        f"EntryBottomPrice={_log_value(trade.get('entry_bottom_price'))} | EntryTopFilled={_log_value(entry_top_filled)} | "
        f"EntryBottomFilled={_log_value(entry_bottom_filled)} | EntryTopTS={_log_value(trade.get('entry_top_ts'))} | "
        f"EntryBottomTS={_log_value(trade.get('entry_bottom_ts'))} | EntryTopShares={_log_value(_safe_int(trade.get('entry_top_shares'), 0))} | "
        f"EntryBottomShares={_log_value(_safe_int(trade.get('entry_bottom_shares'), 0))} | AvgEntry={_log_value(trade.get('avg_entry_price'))} | "
        f"TotalEntryShares={_log_value(_safe_int(trade.get('total_entry_shares'), 0))} | PartialExitCount={_log_value(partial_exit_count)} | "
        f"BOSExitCount={_log_value(bos_exit_count)} | BOS1RefSwingTS={_log_value(trade.get('bos1_ref_swing_ts'))} | "
        f"BOS1ExitTS={_log_value(trade.get('bos1_exit_ts'))} | BOS2RefSwingTS={_log_value(trade.get('bos2_ref_swing_ts'))} | "
        f"BOS2ExitTS={_log_value(trade.get('bos2_exit_ts'))} | BreakevenPrice={_log_value(trade.get('breakeven_price'))} | "
        f"BreakevenExitTriggered={_log_value(bool(trade.get('breakeven_exit_triggered', False)))} | "
        f"InvalidationExitTriggered={_log_value(bool(trade.get('invalidation_exit_triggered', False)))} | "
        f"EODExitTriggered={_log_value(bool(trade.get('eod_exit_triggered', False)))} | ExitTS={_log_value(trade.get('exit_ts'))} | "
        f"ExitPrice={_log_value(trade.get('exit_price'))} | ExitSource={_log_value(trade.get('exit_source'))} | "
        f"ExitReasonFinal={_log_value(trade.get('exit_reason_final'))} | GrossPnL={_log_value(trade.get('gross_pnl'))} | "
        f"GrossPnLPct={_log_value(trade.get('gross_pnl_pct'))} | MAE={_log_value(trade.get('mae'))} | "
        f"MFE={_log_value(trade.get('mfe'))} | BarsHeld={_log_value(_safe_int(trade.get('bars_held'), 0))} | "
        f"HoldingMinutes={_log_value(_safe_int(trade.get('holding_minutes'), 0))} | Result={_log_value(trade.get('result'))} | "
        f"Notes={_log_value(trade.get('notes'))}"
    )


def _compute_final_summary(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_trades = len(trades)
    long_trades = sum(1 for t in trades if str(t.get("side") or "").lower() == "long")
    short_trades = sum(1 for t in trades if str(t.get("side") or "").lower() == "short")

    pnls = [(_safe_float(t.get("gross_pnl"), 0.0) or 0.0) for t in trades]
    pnl_pcts = [(_safe_float(t.get("gross_pnl_pct"), 0.0) or 0.0) for t in trades]
    maes = [(_safe_float(t.get("mae"), 0.0) or 0.0) for t in trades]
    mfes = [(_safe_float(t.get("mfe"), 0.0) or 0.0) for t in trades]
    bars = [float(_safe_int(t.get("bars_held"), 0)) for t in trades]
    mins = [float(_safe_int(t.get("holding_minutes"), 0)) for t in trades]

    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    flats = total_trades - wins - losses

    def avg(vals: List[float]) -> float:
        return (sum(vals) / len(vals)) if vals else 0.0

    return {
        "total_trades": total_trades,
        "long_trades": long_trades,
        "short_trades": short_trades,
        "wins": wins,
        "losses": losses,
        "flats": flats,
        "win_rate": ((wins / total_trades) * 100.0) if total_trades > 0 else 0.0,
        "total_pnl": sum(pnls),
        "avg_pnl": avg(pnls),
        "avg_pnl_pct": avg(pnl_pcts),
        "max_win": max(pnls) if pnls else 0.0,
        "max_loss": min(pnls) if pnls else 0.0,
        "avg_mae": avg(maes),
        "avg_mfe": avg(mfes),
        "avg_bars": avg(bars),
        "avg_minutes": avg(mins),
        "top_only": sum(1 for t in trades if bool(t.get("entry_top_filled", False)) and not bool(t.get("entry_bottom_filled", False))),
        "bottom_only": sum(1 for t in trades if bool(t.get("entry_bottom_filled", False))),
        "both_entries": sum(1 for t in trades if bool(t.get("entry_top_filled", False)) and bool(t.get("entry_bottom_filled", False))),
        "invalid_exits": sum(1 for t in trades if str(t.get("exit_reason_final") or "") == "INVALIDATION"),
        "bos1_exits": sum(1 for t in trades if _safe_int(t.get("partial_exit_count"), 0) >= 1),
        "bos2_exits": sum(1 for t in trades if str(t.get("exit_reason_final") or "") == "BOS2"),
        "be_exits": sum(1 for t in trades if str(t.get("exit_reason_final") or "") == "BREAKEVEN"),
        "eod_exits": sum(1 for t in trades if str(t.get("exit_reason_final") or "") == "EOD"),
    }


def _build_final_summary_log(symbol: str, timeframe: str, trades: List[Dict[str, Any]]) -> str:
    summary = _compute_final_summary(trades)
    return (
        f"[BOS_FVG_V1] FINAL SUMMARY | Symbol={symbol} | TF={timeframe} | TotalTrades={summary['total_trades']} | "
        f"LongTrades={summary['long_trades']} | ShortTrades={summary['short_trades']} | Wins={summary['wins']} | "
        f"Losses={summary['losses']} | Flats={summary['flats']} | WinRate={summary['win_rate']} | "
        f"TotalPnL={summary['total_pnl']} | AvgPnL={summary['avg_pnl']} | AvgPnLPct={summary['avg_pnl_pct']} | "
        f"MaxWin={summary['max_win']} | MaxLoss={summary['max_loss']} | AvgMAE={summary['avg_mae']} | "
        f"AvgMFE={summary['avg_mfe']} | AvgBarsHeld={summary['avg_bars']} | AvgHoldingMinutes={summary['avg_minutes']} | "
        f"TopOnlyTrades={summary['top_only']} | BottomFilledTrades={summary['bottom_only']} | BothEntriesTrades={summary['both_entries']} | "
        f"InvalidationExits={summary['invalid_exits']} | BOS1Exits={summary['bos1_exits']} | BOS2Exits={summary['bos2_exits']} | "
        f"BreakevenExits={summary['be_exits']} | EODExits={summary['eod_exits']}"
    )


def print_bos_fvg_final_summaries() -> None:
    for (symbol, timeframe), state in sorted(_BOS_FVG_STATE.items()):
        if state.get("final_logs_emitted"):
            continue
        trades = state.get("completed_trades") or []
        for t in trades:
            print(_build_final_trade_log(t, symbol, timeframe))
        print(_build_final_summary_log(symbol, timeframe, trades))
        state["final_logs_emitted"] = True


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


def _is_rth_eod(last_candle: Dict[str, Any], last_dt: Optional[datetime], timeframe: str) -> bool:
    session = str(last_candle.get("session") or "").lower()
    if session and session != "rth":
        return False
    ts_et = last_candle.get("ts_et") or _as_et_str(_ts_to_iso(last_dt) if last_dt else None)
    dt_et = _parse_ts(ts_et)
    if dt_et is None:
        return False
    dt_et = dt_et.astimezone(_ET)
    tf_minutes = {
        "1m": 1,
        "3m": 3,
        "5m": 5,
        "15m": 15,
        # 1h candles in this engine are anchored at 09:30 ET.
        # The final (partial) RTH candle is timestamped 15:30 ET and must
        # still be treated as the end-of-day exit event.
        "1h": 60,
    }.get(str(timeframe or "").lower(), 1)
    rth_close_et = dt_et.replace(hour=16, minute=0, second=0, microsecond=0)
    # Some feeds timestamp the final RTH bar at 16:00 ET for 5m/15m data
    # (bar close timestamp). Include equality on the left bound so EOD can
    # still trigger for those bars.
    return dt_et <= rth_close_et <= (dt_et + timedelta(minutes=tf_minutes))


def _get_eod_exit_price(
    last_candle: Dict[str, Any],
    close_px: Optional[float],
    spot_last_candle: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[float], str]:
    for candle in (last_candle or {}, spot_last_candle or {}):
        for k in ("last_1m_rth_close", "last_1m_close", "rth_last_1m_close"):
            v = _safe_float(candle.get(k))
            if v is not None:
                return v, "last_1m_close"
    spot_close = _safe_float((spot_last_candle or {}).get("close"))
    if spot_close is not None:
        return spot_close, "spot_1m_close"
    if close_px is not None:
        return close_px, "close"
    return None, "last_1m_close_missing"


def _trade_list_with_pl(trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for t in trades:
        pnl = _safe_float(t.get("gross_pnl"), 0.0) or 0.0
        out.append({**t, "gross_pnl": pnl, "result": "profit" if pnl > 0 else "loss" if pnl < 0 else "flat"})
    return out


def _trade_summary(state: Dict[str, Any], symbol: Optional[str] = None, timeframe: Optional[str] = None) -> Dict[str, Any]:
    trades = state["completed_trades"]
    total_profit_loss = sum((_safe_float(t.get("gross_pnl"), 0.0) or 0.0) for t in trades)
    wins = sum(1 for t in trades if (_safe_float(t.get("gross_pnl"), 0.0) or 0.0) > 0)
    losses = sum(1 for t in trades if (_safe_float(t.get("gross_pnl"), 0.0) or 0.0) < 0)
    flats = len(trades) - wins - losses
    avg_profit_loss_per_trade = (total_profit_loss / len(trades)) if trades else 0.0
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
        "trade_list": _trade_list_with_pl(trades),
    }


def evaluate_bos_score_v1(
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
        print(f"[BOS_FVG_V1] BOS detected LONG | Symbol={symbol} | TF={timeframe} | Break={recent_high_price}")
    if short_bos_detected and swing_low_key:
        state["broken_swing_lows"].add(swing_low_key)
        print(f"[BOS_FVG_V1] BOS detected SHORT | Symbol={symbol} | TF={timeframe} | Break={recent_low_price}")

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

    # Setup state management: only before first fill/open position
    if not state["open_position"] and cfg["enabled"] and chosen_bos_detected and chosen_score_pass and cfg["max_open_positions"] >= 1:
        pending = state["pending_setup"]
        if pending and pending.get("side") != chosen_side:
            print(f"[BOS_FVG_V1] pending setup canceled opposite BOS | Symbol={symbol} | TF={timeframe} | OldSide={pending.get('side')} | NewSide={chosen_side}")
            state["pending_setup"] = None
        else:
            if pending and pending.get("side") == chosen_side:
                if DEBUG_LOGS:
                    print(f"[BOS_FVG_V1] pending setup replaced newer same-side BOS | Symbol={symbol} | TF={timeframe} | Side={chosen_side}")

            state["trade_id_counter"] += 1
            trade_id = f"{symbol}_{timeframe}_{state['trade_id_counter']}"
            top_shares = ENTRY_LEG_SHARES
            bottom_shares = ENTRY_LEG_SHARES
            total_shares = top_shares + bottom_shares
            state["pending_setup"] = {
                "trade_id": trade_id,
                "side": chosen_side,
                "bos_ts": last_ts,
                "bos_ts_et": last_ts_et,
                "shares_total": total_shares,
                "entry_top_shares": top_shares,
                "entry_bottom_shares": bottom_shares,
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
                "fvg": None,
                "notes": "awaiting_first_same_direction_fvg_after_bos",
            }
            if DEBUG_LOGS:
                print(
                    f"[BOS_FVG_V1] setup armed | Symbol={symbol} | TF={timeframe} | TradeID={trade_id} | "
                    f"Side={chosen_side} | BOS_TS={last_ts} | BOSScore={chosen_score_total:.2f}"
                )

    # FVG discovery
    pending = state["pending_setup"]
    fvg_source = fvgs if isinstance(fvgs, list) else (last_candle.get("fvgs") if isinstance(last_candle.get("fvgs"), list) else [])
    if pending and not pending.get("fvg"):
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
                f"[BOS_FVG_V1] FVG selected | Symbol={symbol} | TF={timeframe} | TradeID={pending.get('trade_id')} | "
                f"Side={pending.get('side')} | FVG_TS={fvg.get('created_ts')} | Low={_safe_float(fvg.get('low'))} | High={_safe_float(fvg.get('high'))} | "
                f"FVGScore={fvg_score} | TradeScore={trade_score} | FVGScoreSrc={fvg_score_src} | TradeScoreSrc={trade_score_src} | "
                f"FVG={json.dumps(fvg, default=str, sort_keys=True)}"
            )
    elif pending and pending.get("fvg"):
        pending["fvg"], _ = _refresh_selected_fvg_fields(pending.get("fvg"), fvg_source or [])

    # Entry fills (touch-based at price level)
    pending = state["pending_setup"]
    if pending and pending.get("fvg") and high_px is not None and low_px is not None:
        if not state["open_position"]:
            f = pending["fvg"]
            fvg_high = _safe_float(f.get("high"))
            fvg_low = _safe_float(f.get("low"))
            state["open_position"] = {
                "trade_id": pending.get("trade_id"), "side": pending.get("side"),
                "bos_ts": pending.get("bos_ts"), "bos_ts_et": pending.get("bos_ts_et"),
                "fvg_ts": f.get("created_ts"), "fvg_high": fvg_high, "fvg_low": fvg_low,
                "fvg_filled": bool(f.get("filled", False)),
                "fvg_filled_ts": f.get("filled_ts"),
                "fvg_score": _safe_float(f.get("fvg_score")),
                "trade_score": _safe_float(f.get("trade_score")),
                "fvg_score_src": f.get("fvg_score_src"),
                "trade_score_src": f.get("trade_score_src"),
                "fvg_after_bos": True,
                "entry_top_price": fvg_high, "entry_bottom_price": fvg_low,
                "entry_top_filled": False, "entry_bottom_filled": False,
                "entry_top_ts": None, "entry_bottom_ts": None,
                "entry_top_shares_planned": _safe_int(pending.get("entry_top_shares"), 0),
                "entry_bottom_shares_planned": _safe_int(pending.get("entry_bottom_shares"), 0),
                "entry_top_shares": 0, "entry_bottom_shares": 0,
                "total_shares_open": 0, "avg_entry_price": None,
                "first_fill_ts": None, "first_fill_ts_et": None,
                "first_opposite_bos_exit_done": False, "opposite_bos_exit_count": 0,
                "bos1_ref_swing_ts": None, "bos1_exit_ts": None,
                "bos2_ref_swing_ts": None, "bos2_exit_ts": None,
                "breakeven_armed_ts": None,
                "consumed_opposite_bos_keys": set(),
                "stop_mode": "fvg_invalidation", "breakeven_price": None,
                "eod_exit_pending_day": None,
                "last_rth_1m_close": None,
                "last_rth_1m_close_ts": None,
                "realized_pnl": 0.0,
                "entry_ref_swing_high": pending.get("entry_ref_swing_high"),
                "entry_ref_swing_high_ts": pending.get("entry_ref_swing_high_ts"),
                "entry_ref_swing_high_score": pending.get("entry_ref_swing_high_score"),
                "entry_ref_swing_low": pending.get("entry_ref_swing_low"),
                "entry_ref_swing_low_ts": pending.get("entry_ref_swing_low_ts"),
                "entry_ref_swing_low_score": pending.get("entry_ref_swing_low_score"),
                "bos_score_total": pending.get("score_total", 0.0),
                "bos_score_threshold": cfg["score_min"],
                "bos_momentum_value": pending.get("mom_value", 0.0),
                "bos_volume_value": pending.get("vol_value", 0.0),
                "bos_close_strength_value": pending.get("close_strength_value", 0.0),
                "bos_break_distance_value": pending.get("break_distance_value", 0.0),
                "structure_state_tf": pending.get("structure_state_tf"),
                "structure_state_15m": pending.get("structure_state_15m"),
                "structure_state_1h": pending.get("structure_state_1h"),
                "highest_price_during_trade": high_px,
                "lowest_price_during_trade": low_px,
                "bars_held": 0,
                "notes": "",
            }

    pos = state.get("open_position")
    if pos and high_px is not None and low_px is not None:
        if not pos.get("first_fill_ts"):
            selected_fvg = {
                "created_ts": pos.get("fvg_ts"),
                "direction": "bull" if pos.get("side") == "long" else "bear",
                "high": pos.get("fvg_high"),
                "low": pos.get("fvg_low"),
                "filled": bool(pos.get("fvg_filled", False)),
                "filled_ts": pos.get("fvg_filled_ts"),
                "fvg_score": pos.get("fvg_score"),
                "trade_score": pos.get("trade_score"),
                "fvg_score_src": pos.get("fvg_score_src"),
                "trade_score_src": pos.get("trade_score_src"),
            }
            refreshed_selected_fvg, _ = _refresh_selected_fvg_fields(selected_fvg, fvg_source or [])
            if isinstance(refreshed_selected_fvg, dict):
                pos["fvg_high"] = _safe_float(refreshed_selected_fvg.get("high"))
                pos["fvg_low"] = _safe_float(refreshed_selected_fvg.get("low"))
                pos["fvg_filled"] = bool(refreshed_selected_fvg.get("filled", False))
                pos["fvg_filled_ts"] = refreshed_selected_fvg.get("filled_ts")
                pos["fvg_score"] = _safe_float(refreshed_selected_fvg.get("fvg_score"))
                pos["trade_score"] = _safe_float(refreshed_selected_fvg.get("trade_score"))
                pos["fvg_score_src"] = refreshed_selected_fvg.get("fvg_score_src")
                pos["trade_score_src"] = refreshed_selected_fvg.get("trade_score_src")
                pos["entry_top_price"] = pos["fvg_high"]
                pos["entry_bottom_price"] = pos["fvg_low"]
        fvg_created_dt = _parse_ts(pos.get("fvg_ts"))
        # Require entry touches to happen strictly after the FVG-formation bar.
        can_fill_entries = not (fvg_created_dt and last_dt and last_dt <= fvg_created_dt)
        side = pos.get("side")
        levels: List[Tuple[str, Optional[float], int]] = []
        if side == "long":
            # Long entries are prioritized from FVG top -> bottom.
            levels = [
                ("top", _safe_float(pos.get("entry_top_price")), _safe_int(pos.get("entry_top_shares_planned"), _safe_int(pos.get("entry_top_shares"), 0))),
                ("bottom", _safe_float(pos.get("entry_bottom_price")), _safe_int(pos.get("entry_bottom_shares_planned"), _safe_int(pos.get("entry_bottom_shares"), 0))),
            ]
        elif side == "short":
            # Short entries are prioritized from FVG bottom -> top (vice versa).
            levels = [
                ("bottom", _safe_float(pos.get("entry_bottom_price")), _safe_int(pos.get("entry_bottom_shares_planned"), _safe_int(pos.get("entry_bottom_shares"), 0))),
                ("top", _safe_float(pos.get("entry_top_price")), _safe_int(pos.get("entry_top_shares_planned"), _safe_int(pos.get("entry_top_shares"), 0))),
            ]

        for leg, px, planned_shares in levels:
            if px is None or planned_shares <= 0:
                continue
            if not can_fill_entries:
                continue
            touched = low_px <= px <= high_px
            already = pos.get(f"entry_{leg}_filled", False)
            if not touched or already:
                continue
            shares = planned_shares
            cost = shares * px
            if cost > state["cash"]:
                if DEBUG_LOGS:
                    print(f"[BOS_FVG_V1] entry leg skipped insufficient_cash | Symbol={symbol} | TF={timeframe} | TradeID={pos.get('trade_id')} | EntryType={leg} | Needed={cost:.2f} | Cash={state['cash']:.2f}")
                continue
            old_total = _safe_int(pos.get("total_shares_open"), 0)
            old_avg = _safe_float(pos.get("avg_entry_price"), 0.0) or 0.0
            new_total = old_total + shares
            new_avg = ((old_avg * old_total) + (px * shares)) / new_total if new_total > 0 else px
            pos["total_shares_open"] = new_total
            pos["avg_entry_price"] = new_avg
            pos[f"entry_{leg}_filled"] = True
            pos[f"entry_{leg}_ts"] = last_ts
            pos[f"entry_{leg}_shares"] = shares
            state["cash"] -= cost
            print(
                f"[BOS_FVG_V1] entry filled | Symbol={symbol} | TF={timeframe} | TradeID={pos.get('trade_id')} | "
                f"Side={side} | Leg={leg} | FillPrice={px} | Shares={shares} | AvgEntry={new_avg} | OpenShares={new_total}"
            )
            if not pos.get("first_fill_ts"):
                pos["first_fill_ts"] = last_ts
                pos["first_fill_ts_et"] = last_ts_et
                state["pending_setup"] = None
                print(
                    f"[BOS_FVG_V1] entry fvg snapshot | Symbol={symbol} | TF={timeframe} | TradeID={pos.get('trade_id')} | "
                    f"FVG_TS={pos.get('fvg_ts')} | Low={_safe_float(pos.get('fvg_low'))} | High={_safe_float(pos.get('fvg_high'))} | "
                    f"FVGScore={_safe_float(pos.get('fvg_score'))} | TradeScore={_safe_float(pos.get('trade_score'))} | "
                    f"FVGScoreSrc={pos.get('fvg_score_src')} | TradeScoreSrc={pos.get('trade_score_src')}"
                )

        if _safe_int(pos.get("total_shares_open"), 0) == 0:
            state["open_position"] = None
        else:
            if pos.get("entry_top_filled") and pos.get("entry_bottom_filled"):
                state["pending_setup"] = None
            status = "in_position"

    # Update excursion
    pos = state.get("open_position")
    if pos:
        if high_px is not None:
            pos["highest_price_during_trade"] = max(_safe_float(pos.get("highest_price_during_trade"), high_px) or high_px, high_px)
        if low_px is not None:
            pos["lowest_price_during_trade"] = min(_safe_float(pos.get("lowest_price_during_trade"), low_px) or low_px, low_px)
        pos["bars_held"] = _safe_int(pos.get("bars_held"), 0) + 1

    def _close_trade(
        exit_price: float,
        exit_reason: str,
        exit_source: str = "",
        exit_ts_override: Optional[str] = None,
        exit_ts_et_override: Optional[str] = None,
    ) -> None:
        nonlocal status
        p = state.get("open_position")
        if not p:
            return
        shares_open = _safe_int(p.get("total_shares_open"), 0)
        if shares_open <= 0:
            state["open_position"] = None
            return

        side = p.get("side", "long")
        avg_entry = _safe_float(p.get("avg_entry_price"), 0.0) or 0.0
        if side == "short":
            pnl_close = (avg_entry - exit_price) * shares_open
        else:
            pnl_close = (exit_price - avg_entry) * shares_open
        total_pnl = (_safe_float(p.get("realized_pnl"), 0.0) or 0.0) + pnl_close
        state["cash"] += shares_open * exit_price

        entry_dt = _parse_ts(p.get("first_fill_ts"))
        final_exit_ts = exit_ts_override or last_ts
        final_exit_ts_et = exit_ts_et_override or last_ts_et
        exit_dt = _parse_ts(final_exit_ts)
        holding_minutes = int((exit_dt - entry_dt).total_seconds() // 60) if (entry_dt and exit_dt) else 0
        lowest = _safe_float(p.get("lowest_price_during_trade"), exit_price) or exit_price
        highest = _safe_float(p.get("highest_price_during_trade"), exit_price) or exit_price

        trade = {
            "trade_id": p.get("trade_id"), "symbol": symbol, "timeframe": timeframe, "side": side,
            "bos_ts": p.get("bos_ts"), "bos_ts_et": p.get("bos_ts_et"), "fvg_ts": p.get("fvg_ts"),
            "fvg_high": p.get("fvg_high"), "fvg_low": p.get("fvg_low"), "fvg_after_bos": True,
            "fvg_score": p.get("fvg_score"), "trade_score": p.get("trade_score"),
            "fvg_score_src": p.get("fvg_score_src"), "trade_score_src": p.get("trade_score_src"),
            "entry_type": "both" if p.get("entry_top_filled") and p.get("entry_bottom_filled") else "top" if p.get("entry_top_filled") else "bottom" if p.get("entry_bottom_filled") else "none",
            "entry_top_price": p.get("entry_top_price"), "entry_bottom_price": p.get("entry_bottom_price"),
            "entry_top_filled": p.get("entry_top_filled"), "entry_bottom_filled": p.get("entry_bottom_filled"),
            "entry_top_ts": p.get("entry_top_ts"), "entry_bottom_ts": p.get("entry_bottom_ts"),
            "entry_top_shares": _safe_int(p.get("entry_top_shares"), 0), "entry_bottom_shares": _safe_int(p.get("entry_bottom_shares"), 0),
            "avg_entry_price": avg_entry, "total_entry_shares": _safe_int(p.get("entry_top_shares"), 0) + _safe_int(p.get("entry_bottom_shares"), 0),
            "entry_ts": p.get("first_fill_ts"), "entry_ts_et": p.get("first_fill_ts_et"),
            "exit_ts": final_exit_ts, "exit_ts_et": final_exit_ts_et, "exit_price": exit_price,
            "exit_reason_final": exit_reason, "exit_source": exit_source,
            "partial_exit_count": 1 if p.get("first_opposite_bos_exit_done") else 0,
            "bos_exit_count": p.get("opposite_bos_exit_count", 0),
            "bos1_ref_swing_ts": p.get("bos1_ref_swing_ts"),
            "bos1_exit_ts": p.get("bos1_exit_ts"),
            "bos2_ref_swing_ts": p.get("bos2_ref_swing_ts"),
            "bos2_exit_ts": p.get("bos2_exit_ts"),
            "breakeven_price": p.get("breakeven_price"),
            "breakeven_exit_triggered": exit_reason == "BREAKEVEN",
            "invalidation_exit_triggered": exit_reason == "INVALIDATION",
            "eod_exit_triggered": exit_reason == "EOD",
            "gross_pnl": total_pnl,
            "gross_pnl_pct": ((total_pnl / (avg_entry * (_safe_int(p.get("entry_top_shares"), 0) + _safe_int(p.get("entry_bottom_shares"), 0)))) * 100.0) if avg_entry > 0 and (_safe_int(p.get("entry_top_shares"), 0) + _safe_int(p.get("entry_bottom_shares"), 0)) > 0 else 0.0,
            "result": "profit" if total_pnl > 0 else "loss" if total_pnl < 0 else "flat",
            "bars_held": p.get("bars_held", 0), "holding_minutes": holding_minutes,
            "mae": (avg_entry - lowest) if side == "long" else (highest - avg_entry),
            "mfe": (highest - avg_entry) if side == "long" else (avg_entry - lowest),
            "structure_state_tf": p.get("structure_state_tf"), "structure_state_15m": p.get("structure_state_15m"), "structure_state_1h": p.get("structure_state_1h"),
            "bos_score_total": p.get("bos_score_total"),
            "bos_close_threshold": cfg.get("close_threshold"), "bos_break_threshold": cfg.get("break_threshold"),
            "bos_momentum_value": p.get("bos_momentum_value"), "bos_volume_value": p.get("bos_volume_value"),
            "bos_close_strength_value": p.get("bos_close_strength_value"), "bos_break_distance_value": p.get("bos_break_distance_value"),
            "entry_ref_swing_high": p.get("entry_ref_swing_high"),
            "entry_ref_swing_high_ts": p.get("entry_ref_swing_high_ts"),
            "entry_ref_swing_high_score": p.get("entry_ref_swing_high_score"),
            "entry_ref_swing_low": p.get("entry_ref_swing_low"),
            "entry_ref_swing_low_ts": p.get("entry_ref_swing_low_ts"),
            "entry_ref_swing_low_score": p.get("entry_ref_swing_low_score"),
            "notes": p.get("notes", ""),
        }
        print(
            f"[BOS_FVG_V1] trade closed | Symbol={symbol} | TF={timeframe} | TradeID={p.get('trade_id')} | "
            f"Side={side} | ExitReason={exit_reason} | ExitSource={exit_source} | ExitPrice={exit_price} | GrossPnL={total_pnl}"
        )
        pending_now = state.get("pending_setup")
        if pending_now and pending_now.get("trade_id") == p.get("trade_id"):
            state["pending_setup"] = None
        state["completed_trades"].append(trade)
        state["open_position"] = None
        status = "exited"

    # Exit checks
    pos = state.get("open_position")
    if pos and close_px is not None:
        fvg_source = fvgs if isinstance(fvgs, list) else (last_candle.get("fvgs") if isinstance(last_candle.get("fvgs"), list) else [])
        for f in fvg_source or []:
            if (
                _normalize_fvg_direction(f.get("direction")) == _normalize_fvg_direction(pos.get("side"))
                and f.get("created_ts") == pos.get("fvg_ts")
            ):
                pos["fvg_filled"] = bool(f.get("filled", False))
                pos["fvg_filled_ts"] = f.get("filled_ts")
                break
        last_1m_rth_close, last_1m_rth_close_source = _get_eod_exit_price(last_candle, close_px, spot_last_candle)
        if str(last_candle.get("session") or "").lower() == "rth" and last_1m_rth_close is not None and last_dt is not None:
            pos["last_rth_1m_close"] = last_1m_rth_close
            pos["last_rth_1m_close_ts"] = last_ts
            pos["last_rth_1m_close_source"] = last_1m_rth_close_source
            pos["last_rth_day"] = last_dt.astimezone(_ET).date().isoformat()

        current_day_et = last_dt.astimezone(_ET).date().isoformat() if last_dt else None
        last_rth_day = pos.get("last_rth_day")
        if current_day_et and last_rth_day and current_day_et > last_rth_day and _safe_int(pos.get("total_shares_open"), 0) > 0:
            eod_px = _safe_float(pos.get("last_rth_1m_close"))
            eod_ts = pos.get("last_rth_1m_close_ts")
            eod_source = str(pos.get("last_rth_1m_close_source") or "last_1m_close")
            if eod_px is not None:
                _close_trade(eod_px, "EOD", eod_source, eod_ts, _as_et_str(eod_ts))
                pos = state.get("open_position")
                if not pos:
                    close_px = None

    pos = state.get("open_position")
    if pos and close_px is not None:
        side = pos.get("side", "long")
        fvg_low = _safe_float(pos.get("fvg_low"))
        fvg_high = _safe_float(pos.get("fvg_high"))

        # Opposite BOS exits (post-entry only)
        first_fill_dt = _parse_ts(pos.get("first_fill_ts"))
        if chosen_bos_detected and first_fill_dt and last_dt and last_dt > first_fill_dt:
            is_opposite = (side == "long" and chosen_side == "short") or (side == "short" and chosen_side == "long")
            if is_opposite:
                bos_key = f"{last_ts}|{chosen_side}"
                consumed = pos.get("consumed_opposite_bos_keys") or set()
                if bos_key not in consumed:
                    consumed.add(bos_key)
                    pos["consumed_opposite_bos_keys"] = consumed
                    pos["opposite_bos_exit_count"] = _safe_int(pos.get("opposite_bos_exit_count"), 0) + 1
                    shares_open = _safe_int(pos.get("total_shares_open"), 0)
                    if pos["opposite_bos_exit_count"] == 1 and shares_open > 0:
                        exit_shares = max(1, shares_open // 2)
                        avg_entry = _safe_float(pos.get("avg_entry_price"), 0.0) or 0.0
                        pnl_partial = (close_px - avg_entry) * exit_shares if side == "long" else (avg_entry - close_px) * exit_shares
                        pos["realized_pnl"] = (_safe_float(pos.get("realized_pnl"), 0.0) or 0.0) + pnl_partial
                        state["cash"] += exit_shares * close_px
                        pos["total_shares_open"] = shares_open - exit_shares
                        pos["first_opposite_bos_exit_done"] = True
                        pos["bos1_exit_ts"] = last_ts
                        pos["bos1_ref_swing_ts"] = recent_high_ts if chosen_side == "long" else recent_low_ts if chosen_side == "short" else None
                        pos["stop_mode"] = "breakeven"
                        pos["breakeven_price"] = avg_entry
                        pos["breakeven_armed_ts"] = last_ts
                        print(
                            f"[BOS_FVG_V1] partial exit BOS1 | Symbol={symbol} | TF={timeframe} | TradeID={pos.get('trade_id')} | "
                            f"Side={side} | ExitReason=BOS1 | ExitPct=50 | ExitPrice={close_px} | ExitShares={exit_shares} | "
                            f"RemainingSize={pos.get('total_shares_open')} | New_SL=BE | BreakevenPrice={avg_entry} | RealizedPnL={pnl_partial}"
                        )
                        if _safe_int(pos.get("total_shares_open"), 0) <= 0:
                            _close_trade(close_px, "BOS1", "close")
                    elif pos["opposite_bos_exit_count"] >= 2 and _safe_int(pos.get("total_shares_open"), 0) > 0:
                        pos["bos2_exit_ts"] = last_ts
                        pos["bos2_ref_swing_ts"] = recent_high_ts if chosen_side == "long" else recent_low_ts if chosen_side == "short" else None
                        _close_trade(close_px, "BOS2", "close")

        pos = state.get("open_position")
        if pos and pos.get("stop_mode") == "breakeven" and _safe_int(pos.get("total_shares_open"), 0) > 0:
            be = _safe_float(pos.get("breakeven_price"))
            if be is not None:
                be_armed_ts = pos.get("breakeven_armed_ts")
                be_same_candle_as_arm = bool(be_armed_ts and last_ts and be_armed_ts == last_ts)
                be_hit = False
                if not be_same_candle_as_arm:
                    be_hit = (side == "long" and low_px is not None and low_px <= be) or (side == "short" and high_px is not None and high_px >= be)
                if be_hit:
                    if DEBUG_LOGS:
                        print(
                            f"[BOS_FVG_V1] breakeven stop hit | Symbol={symbol} | TF={timeframe} | TradeID={pos.get('trade_id')} | "
                            f"Side={side} | BreakevenPrice={be} | OpenShares={pos.get('total_shares_open')}"
                        )
                    _close_trade(be, "BREAKEVEN", "breakeven")

        pos = state.get("open_position")
        if pos and _safe_int(pos.get("total_shares_open"), 0) > 0:
            invalid = False
            fvg_filled = False
            pos_fvg_filled_ts = pos.get("fvg_filled_ts")
            if pos_fvg_filled_ts:
                ts_filled = _parse_ts(pos_fvg_filled_ts)
                fvg_filled = bool(ts_filled and last_dt and ts_filled <= last_dt)
            if not fvg_filled:
                fvg_filled = bool(pos.get("fvg_filled"))
            if side == "long" and fvg_low is not None and close_px < fvg_low and fvg_filled:
                invalid = True
            if side == "short" and fvg_high is not None and close_px > fvg_high and fvg_filled:
                invalid = True
            if invalid:
                _close_trade(close_px, "INVALIDATION", "close")

        pos = state.get("open_position")
        if pos and _safe_int(pos.get("total_shares_open"), 0) > 0 and _is_rth_eod(last_candle, last_dt, timeframe):
            eod_px, eod_source = _get_eod_exit_price(last_candle, close_px, spot_last_candle)
            if eod_px is not None:
                _close_trade(eod_px, "EOD", eod_source)
            else:
                print(
                    f"[BOS_FVG_V1] EOD exit skipped missing last_1m_close | Symbol={symbol} | TF={timeframe} | "
                    f"TradeID={pos.get('trade_id')} | ExitReason=EOD | ExitSource=last_1m_close"
                )

    if not cfg["enabled"]:
        status = "disabled"
        skip_reason = "bos_score_disabled"
    elif state.get("open_position"):
        status = "in_position"
        skip_reason = "position_already_open"
    elif state.get("pending_setup"):
        status = "pending_setup"

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

    summary = _trade_summary(state, symbol, timeframe)
    snapshot = {
        "id": "bos_fvg_v1",
        "symbol": symbol,
        "timeframe": timeframe,
        "last_eval_ts": last_ts,
        "last_eval_ts_et": last_ts_et,
        "status": status,
        "cash": state["cash"],
        "position_open": state["open_position"] is not None,
        "open_position_side": (state["open_position"] or {}).get("side"),
        "pending_setup_side": (state["pending_setup"] or {}).get("side"),
        "pending_setup": deepcopy(state["pending_setup"]),
        "pending_entry": None,
        "last_signal": deepcopy(state["signals"][-1] if state["signals"] else None),
        "last_trade": deepcopy(state["completed_trades"][-1] if state["completed_trades"] else None),
        "trade_count": len(state["completed_trades"]),
        "signal_count": len(state["signals"]),
        "trade_summary": summary,
        "trade_list": summary["trade_list"],
        "open_position_summary": deepcopy(state["open_position"]),
    }
    state["latest_snapshot"] = snapshot
    return snapshot
