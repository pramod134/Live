# skinny_snapshot_builder.py
#
# Builds a compact multi-timeframe snapshot for GPT/trade-planning strategies.
# Pure module: no DB, no network, no execution.

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Tuple

DEFAULT_INCLUDE_TFS = ["1d", "1h", "15m", "5m", "1m"]
FVG_FRESH_BARS = 12
FVG_STALE_BARS = 50


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        return float(x)
    except (TypeError, ValueError):
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except (TypeError, ValueError):
        return default


def _round(x: Any, ndigits: int = 4) -> Optional[float]:
    v = _safe_float(x)
    return None if v is None else round(v, ndigits)


def _dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []


def _now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _iso_to_dt(ts: Any) -> Optional[dt.datetime]:
    if not isinstance(ts, str) or not ts:
        return None
    try:
        t = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=dt.timezone.utc)
        return t.astimezone(dt.timezone.utc)
    except Exception:
        return None


def _seconds_between(a_iso: Optional[str], b_iso: Optional[str]) -> Optional[int]:
    a = _iso_to_dt(a_iso)
    b = _iso_to_dt(b_iso)
    if not a or not b:
        return None
    return int(abs((a - b).total_seconds()))


def _tf_to_seconds(tf: Any) -> Optional[int]:
    s = str(tf or "").strip().lower()
    if not s:
        return None
    try:
        if s.endswith("m"):
            return int(s[:-1]) * 60
        if s.endswith("h"):
            return int(s[:-1]) * 60 * 60
        if s.endswith("d"):
            return int(s[:-1]) * 24 * 60 * 60
        if s.endswith("w"):
            return int(s[:-1]) * 7 * 24 * 60 * 60
        if s.isdigit():
            return int(s) * 60
    except Exception:
        return None
    return None


def _atr_buffers(atr: Optional[float]) -> Dict[str, Optional[float]]:
    a = _safe_float(atr)
    if a is None or a <= 0:
        return {"tight": None, "normal": None, "wide": None}
    return {
        "tight": _round(a * 0.10),
        "normal": _round(a * 0.25),
        "wide": _round(a * 0.40),
    }


def _level_with_buffers(price: Any, atr: Optional[float]) -> Dict[str, Any]:
    p = _safe_float(price)
    buffers = _atr_buffers(atr)
    out: Dict[str, Any] = {
        "price": _round(p),
        "atr_buffer": buffers,
    }
    if p is not None:
        normal = _safe_float(buffers.get("normal"))
        if normal is not None:
            out["long_stop_ref"] = _round(p - normal)
            out["short_stop_ref"] = _round(p + normal)
    return out


def _direction_from_state(state: Any) -> str:
    s = str(state or "").lower()
    if "bull" in s:
        return "bullish"
    if "bear" in s:
        return "bearish"
    return "neutral"


def _event_truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (dict, list, tuple, set)):
        return len(v) > 0
    if isinstance(v, str):
        return bool(v.strip())
    return bool(v)


# ---------------------------------------------------------------------------
# Decision price
# ---------------------------------------------------------------------------

def _get_1m_close(snapshots_by_tf: Dict[str, Dict[str, Any]]) -> Tuple[Optional[float], Optional[str]]:
    snap_1m = _dict(snapshots_by_tf.get("1m"))
    candle = _dict(snap_1m.get("last_candle"))
    return _safe_float(candle.get("close")), candle.get("ts")


def build_decision_price(
    snapshots_by_tf: Dict[str, Dict[str, Any]],
    *,
    live_price: Optional[float] = None,
    live_ts: Optional[str] = None,
    decision_ts: Optional[str] = None,
) -> Dict[str, Any]:
    decision_ts = decision_ts or _now_utc_iso()
    lp = _safe_float(live_price)
    if lp is not None:
        return {
            "price": _round(lp),
            "source": "live",
            "ts": live_ts or decision_ts,
            "decision_ts": decision_ts,
            "staleness_sec": _seconds_between(decision_ts, live_ts or decision_ts),
        }

    close_1m, ts_1m = _get_1m_close(snapshots_by_tf)
    return {
        "price": _round(close_1m),
        "source": "1m_close" if close_1m is not None else "unavailable",
        "ts": ts_1m,
        "decision_ts": decision_ts,
        "staleness_sec": _seconds_between(decision_ts, ts_1m),
    }


# ---------------------------------------------------------------------------
# tick_tf extractors
# ---------------------------------------------------------------------------

def _extract_last_candle(candle: Dict[str, Any]) -> Dict[str, Any]:
    cluster = _dict(candle.get("cluster"))
    return {
        "ts": candle.get("ts"),
        "time_et": candle.get("time_et"),
        "open": _round(candle.get("open")),
        "high": _round(candle.get("high")),
        "low": _round(candle.get("low")),
        "close": _round(candle.get("close")),
        "direction": candle.get("direction"),
        "shape": candle.get("shape"),
        "body": _round(candle.get("body")),
        "range": _round(candle.get("range")),
        "upper_wick": _round(candle.get("upper_wick")),
        "lower_wick": _round(candle.get("lower_wick")),
        "vol_rel": _round(candle.get("vol_rel")),
        "is_high_vol": bool(candle.get("is_high_vol", False)),
        "is_low_vol": bool(candle.get("is_low_vol", False)),
        "mom_atr": _round(candle.get("mom_atr")),
        "spread_strength": _round(candle.get("spread_strength")),
        "cluster": {
            "state": cluster.get("state"),
            "avg_mom_atr": _round(cluster.get("avg_mom_atr")),
            "avg_vol_rel": _round(cluster.get("avg_vol_rel")),
        },
    }


def _extract_structure(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    ea = _dict(snapshot.get("extras_advanced"))
    meta = _dict(snapshot.get("structure_meta") or ea.get("structure_meta"))
    state = snapshot.get("structure_state") or ea.get("structure_state")
    return {
        "state": state,
        "raw": snapshot.get("structure_state_raw") or meta.get("raw_state"),
        "direction": meta.get("direction") or _direction_from_state(state),
        "action": meta.get("action"),
        "confidence": meta.get("confidence"),
        "risk_tier": meta.get("risk_tier"),
        "tags": meta.get("tags") or [],
        "momentum_exhaustion": meta.get("momentum_exhaustion"),
        "htf_alignment": meta.get("htf_alignment"),
        "vwap_context": meta.get("vwap_context"),
    }


def _extract_trend(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    trend = _dict(snapshot.get("trend"))
    return {
        "state": trend.get("state"),
        "ema50": _round(trend.get("ema50")),
        "ema200": _round(trend.get("ema200")),
        "dist_ema50_pct": _round(trend.get("dist_ema50_pct"), 5),
        "dist_ema200_pct": _round(trend.get("dist_ema200_pct"), 5),
    }


def _extract_momentum(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    m = _dict(_dict(snapshot.get("extras_advanced")).get("momentum"))
    return {
        "rsi_14": _round(m.get("rsi_14"), 2),
        "rsi_regime": m.get("rsi_regime"),
        "rsi_slope_3": _round(m.get("rsi_slope_3"), 3),
        "rsi_shift": m.get("rsi_shift"),
        "mom_norm": _round(m.get("mom_norm"), 3),
        "mom_regime": m.get("mom_regime"),
        "mom_strength": m.get("mom_strength"),
        "mom_slope_3": _round(m.get("mom_slope_3"), 3),
        "mom_exhaustion": m.get("mom_exhaustion"),
        "atr": _round(m.get("atr_for_norm")),
    }


def _extract_vwap(snapshot: Dict[str, Any], decision_price: Optional[float]) -> Dict[str, Any]:
    ea = _dict(snapshot.get("extras_advanced"))
    vwap = _dict(ea.get("vwap"))
    atr = _safe_float(_dict(ea.get("momentum")).get("atr_for_norm"))
    v = _safe_float(vwap.get("vwap_rth"))
    dp = _safe_float(decision_price)
    side = None
    dist_atr = None
    if v is not None and dp is not None:
        side = "above" if dp > v else "below" if dp < v else "at"
        if atr and atr > 0:
            dist_atr = (dp - v) / atr
    return {
        "vwap": _round(v),
        "side": side,
        "dist_pct": _round(vwap.get("dist_from_vwap_pct"), 5),
        "dist_atr_decision": _round(dist_atr, 3),
        "extended_flag": vwap.get("extended_flag"),
    }


def _compact_level(level: Any, atr: Optional[float] = None) -> Optional[Dict[str, Any]]:
    if not isinstance(level, dict):
        return None
    price = level.get("price") or level.get("level")
    level_info = _level_with_buffers(price, atr)
    return {
        **level_info,
        "state": level.get("state"),
        "type": level.get("type") or level.get("kind"),
        "ts": level.get("ts") or level.get("ts_ref"),
        "wick_count": level.get("wick_count"),
        "break_count": level.get("break_count"),
        "break_close_count": level.get("break_close_count"),
    }


def _extract_liquidity(snapshot: Dict[str, Any], atr: Optional[float] = None) -> Dict[str, Any]:
    liq = _dict(_dict(snapshot.get("extras_advanced")).get("liq_summary"))
    liq_lite = _dict(snapshot.get("liquidity_lite"))
    return {
        "eq_highs": _safe_int(liq.get("eq_high_stack_count")),
        "eq_lows": _safe_int(liq.get("eq_low_stack_count")),
        "nearest_clean_high": _level_with_buffers(liq.get("nearest_clean_high_price"), atr),
        "nearest_clean_low": _level_with_buffers(liq.get("nearest_clean_low_price"), atr),
        "nearest_clean_high_dist_pct": _round(liq.get("nearest_clean_high_dist_pct"), 5),
        "nearest_clean_low_dist_pct": _round(liq.get("nearest_clean_low_dist_pct"), 5),
        "nearest_intact_above": _compact_level(liq_lite.get("nearest_intact_above"), atr),
        "nearest_intact_below": _compact_level(liq_lite.get("nearest_intact_below"), atr),
        "nearest_swept_above": _compact_level(liq_lite.get("nearest_swept_above"), atr),
        "nearest_swept_below": _compact_level(liq_lite.get("nearest_swept_below"), atr),
    }


def _extract_structural_levels(snapshot: Dict[str, Any], atr: Optional[float] = None) -> Dict[str, Any]:
    s = _dict(snapshot.get("structural_lite"))

    def point(p: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(p, dict):
            return None
        swing = _dict(p.get("swing"))
        level_info = _level_with_buffers(p.get("price"), atr)
        return {
            "label": p.get("label"),
            **level_info,
            "state": swing.get("state"),
            "ts": p.get("ts"),
        }

    return {
        "latest_hh": point(s.get("latest_hh")),
        "latest_hl": point(s.get("latest_hl")),
        "latest_lh": point(s.get("latest_lh")),
        "latest_ll": point(s.get("latest_ll")),
        "nearest_high_above": point(s.get("nearest_high_above")),
        "nearest_low_below": point(s.get("nearest_low_below")),
    }


def _fvg_freshness(created_ts: Any, asof_ts: Any, tf: str) -> Dict[str, Any]:
    age_sec = _seconds_between(asof_ts, created_ts) if created_ts else None
    tf_sec = _tf_to_seconds(tf)
    age_bars = None
    if age_sec is not None and tf_sec and tf_sec > 0:
        age_bars = int(age_sec // tf_sec)

    if age_bars is None:
        freshness = "unknown"
        age_weight = None
    elif age_bars <= FVG_FRESH_BARS:
        freshness = "fresh"
        age_weight = 1.0
    elif age_bars <= FVG_STALE_BARS:
        freshness = "valid"
        age_weight = max(0.35, 1.0 - ((age_bars - FVG_FRESH_BARS) / max(1, FVG_STALE_BARS - FVG_FRESH_BARS)) * 0.65)
    else:
        freshness = "stale"
        age_weight = 0.25

    return {
        "age_sec": age_sec,
        "age_bars": age_bars,
        "freshness": freshness,
        "age_weight": _round(age_weight, 3),
    }


def _compact_fvg(f: Any, *, tf: str, asof_ts: Any, atr: Optional[float] = None) -> Optional[Dict[str, Any]]:
    if not isinstance(f, dict):
        return None
    style = _dict(f.get("style"))
    low = _safe_float(f.get("low"))
    high = _safe_float(f.get("high"))
    return {
        "direction": f.get("direction"),
        "low": _level_with_buffers(low, atr),
        "high": _level_with_buffers(high, atr),
        "trade_score": _round(f.get("trade_score"), 1),
        "fvg_score": _round(f.get("fvg_score"), 1),
        "touch_count": f.get("touch_count"),
        "filled_pct": _round(f.get("filled_pct"), 1),
        "style": style.get("label"),
        "style_confidence": style.get("confidence"),
        "score_status": f.get("score_status"),
        "created_ts": f.get("created_ts"),
        "first_touch_ts": f.get("first_touch_ts"),
        "filled_ts": f.get("filled_ts"),
        "freshness": _fvg_freshness(f.get("created_ts"), asof_ts, tf),
    }


def _extract_top_fvgs(
    snapshot: Dict[str, Any],
    *,
    tf: str,
    asof_ts: Any,
    atr: Optional[float] = None,
    limit_each_side: int = 2,
) -> Dict[str, Any]:
    f = _dict(snapshot.get("fvgs_lite"))
    bull_below = [_compact_fvg(x, tf=tf, asof_ts=asof_ts, atr=atr) for x in _list(f.get("bearish_below"))[:limit_each_side]]
    bear_above = [_compact_fvg(x, tf=tf, asof_ts=asof_ts, atr=atr) for x in _list(f.get("bullish_above"))[:limit_each_side]]
    return {
        "bullish_below": [x for x in bull_below if x],
        "bearish_above": [x for x in bear_above if x],
    }


def _compact_vp_node(node: Any, atr: Optional[float] = None) -> Optional[Dict[str, Any]]:
    if not isinstance(node, dict):
        return None
    return {
        "low": _round(node.get("low")),
        "poc": _level_with_buffers(node.get("poc"), atr),
        "high": _round(node.get("high")),
        "rank": node.get("rank"),
        "final_score": _round(node.get("final_score"), 1),
        "tags": node.get("tags") or [],
    }


def _extract_vp(
    snapshot: Dict[str, Any],
    atr: Optional[float] = None,
    profile_names: Iterable[str] = ("session", "daily_extremes", "structural"),
) -> Dict[str, Any]:
    profiles = _dict(_dict(snapshot.get("volume_profile_lite")).get("profiles"))
    out: Dict[str, Any] = {}
    for name in profile_names:
        p = _dict(profiles.get(name))
        if not p:
            continue
        out[name] = {
            "poc": _level_with_buffers(p.get("poc"), atr),
            "poc2": _level_with_buffers(p.get("poc2"), atr),
            "hvn_here": _compact_vp_node(p.get("nearest_hvn_containing_price"), atr),
            "hvn_above": _compact_vp_node(p.get("nearest_hvn_above"), atr),
            "hvn_below": _compact_vp_node(p.get("nearest_hvn_below"), atr),
            "lvn_above": _compact_vp_node(p.get("nearest_lvn_above"), atr),
            "lvn_below": _compact_vp_node(p.get("nearest_lvn_below"), atr),
        }
    return out


def _extract_vol_context(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    vc = _dict(_dict(snapshot.get("extras_advanced")).get("vol_context"))
    return {"bar_range_vs_atr": _round(vc.get("bar_range_vs_atr"), 3)}


# ---------------------------------------------------------------------------
# spot_events extractors
# ---------------------------------------------------------------------------

def _compact_event(ev: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(ev, dict):
        return None
    meta = _dict(ev.get("meta"))
    return {
        "type": ev.get("type"),
        "ts": ev.get("ts"),
        "score": _round(ev.get("score"), 1),
        "ref_level": _round(meta.get("ref_level") or meta.get("level") or meta.get("level_center")),
        "ref_ts": meta.get("ref_ts") or meta.get("ts_ref"),
        "break_close": _round(meta.get("break_close")),
        "mom_atr": _round(meta.get("mom_atr"), 3),
        "vol_rel": _round(meta.get("vol_rel"), 3),
        "spread_strength": _round(meta.get("spread_strength"), 3),
    }


def _extract_latest_events(event_state: Dict[str, Any]) -> Dict[str, Any]:
    latest = _dict(event_state.get("events_latest"))
    out: Dict[str, Any] = {}
    for key, ev in latest.items():
        if not _event_truthy(ev):
            continue
        compact = _compact_event(ev)
        if compact:
            out[key] = compact
    return out


def _compact_tracker(t: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(t, dict):
        return None
    avwap = _dict(t.get("avwap"))
    return {
        "type": t.get("type"),
        "anchor_event": t.get("anchor_event"),
        "anchor_ts": t.get("anchor_ts"),
        "ref_level": _round(t.get("ref_level")),
        "ref_ts": t.get("ref_ts"),
        "bars": t.get("bars"),
        "status": t.get("status"),
        "structure_state": t.get("structure_state"),
        "vwap_state": t.get("vwap_state"),
        "avwap": _round(avwap.get("vwap")),
        "expired_reason": t.get("expired_reason"),
    }


def _extract_active_trackers(event_state: Dict[str, Any]) -> Dict[str, Any]:
    active = _dict(event_state.get("events_active"))
    out: Dict[str, Any] = {}
    for slot, slot_obj in active.items():
        if not isinstance(slot_obj, dict):
            continue
        a = _compact_tracker(slot_obj.get("active"))
        e = _compact_tracker(slot_obj.get("expired"))
        if a or e:
            out[slot] = {"active": a, "expired": e}
    return out


def _extract_recent_events(event_state: Dict[str, Any], limit: int = 8) -> List[Dict[str, Any]]:
    compact = []
    for ev in _list(event_state.get("events_recent"))[-limit:]:
        c = _compact_event(ev)
        if c:
            compact.append(c)
    return compact


# ---------------------------------------------------------------------------
# Public builders
# ---------------------------------------------------------------------------

def build_tf_skinny_snapshot(
    *,
    tf: str,
    snapshot: Dict[str, Any],
    event_state: Optional[Dict[str, Any]] = None,
    decision_price: Optional[float] = None,
) -> Dict[str, Any]:
    snapshot = _dict(snapshot)
    event_state = _dict(event_state)
    last = _dict(snapshot.get("last_candle"))
    momentum = _extract_momentum(snapshot)
    atr = _safe_float(momentum.get("atr"))
    dp = _safe_float(decision_price)
    last_close = _safe_float(last.get("close"))

    decision_dist_atr = None
    if dp is not None and last_close is not None and atr and atr > 0:
        decision_dist_atr = (dp - last_close) / atr

    return {
        "tf": tf,
        "asof": snapshot.get("asof") or last.get("ts"),
        "last_candle": _extract_last_candle(last),
        "decision_context": {
            "price": _round(dp),
            "dist_from_last_close_atr": _round(decision_dist_atr, 3),
        },
        "trend": _extract_trend(snapshot),
        "structure": _extract_structure(snapshot),
        "momentum": momentum,
        "vwap": _extract_vwap(snapshot, dp),
        "vol_context": _extract_vol_context(snapshot),
        "atr_buffers": _atr_buffers(atr),
        "liquidity": _extract_liquidity(snapshot, atr),
        "structural_levels": _extract_structural_levels(snapshot, atr),
        "fvgs": _extract_top_fvgs(snapshot, tf=tf, asof_ts=snapshot.get("asof") or last.get("ts"), atr=atr),
        "volume_profile": _extract_vp(snapshot, atr),
        "events": {
            "latest": _extract_latest_events(event_state),
            "active_trackers": _extract_active_trackers(event_state),
            "recent": _extract_recent_events(event_state),
        },
    }


def build_snapshot_summary(tfs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    weights = {"1w": 6, "1d": 5, "1h": 4, "15m": 3, "5m": 2, "3m": 1.5, "1m": 1}
    bull = 0.0
    bear = 0.0
    notes = []

    for tf, block in tfs.items():
        w = weights.get(tf, 1.0)
        structure = _dict(block.get("structure"))
        trend = _dict(block.get("trend"))
        direction = structure.get("direction")
        action = structure.get("action")
        confidence = _safe_float(structure.get("confidence"), 50.0) or 50.0
        score = w * max(0.25, confidence / 50.0)

        if direction == "bullish" or trend.get("state") == "bull":
            bull += score
            notes.append(f"{tf}:bullish:{action or 'na'}")
        elif direction == "bearish" or trend.get("state") == "bear":
            bear += score
            notes.append(f"{tf}:bearish:{action or 'na'}")
        else:
            notes.append(f"{tf}:neutral:{action or 'na'}")

    if bull > bear * 1.15:
        bias = "bullish"
    elif bear > bull * 1.15:
        bias = "bearish"
    else:
        bias = "mixed"

    total = bull + bear
    confidence = round((max(bull, bear) / total) * 100, 1) if total > 0 else 0.0
    return {
        "bias": bias,
        "bull_score": round(bull, 2),
        "bear_score": round(bear, 2),
        "confidence": confidence,
        "notes": notes,
        "mtf_alignment": build_mtf_alignment(tfs),
    }


def build_mtf_alignment(tfs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    weights = {"1w": 6, "1d": 5, "1h": 4, "15m": 3, "5m": 2, "3m": 1.5, "1m": 1}
    bull = 0.0
    bear = 0.0
    neutral = 0.0
    aligned_tfs: List[str] = []
    conflict_tfs: List[str] = []

    directions: Dict[str, str] = {}
    for tf, block in tfs.items():
        direction = _dict(block.get("structure")).get("direction") or "neutral"
        directions[tf] = direction
        w = weights.get(tf, 1.0)
        if direction == "bullish":
            bull += w
        elif direction == "bearish":
            bear += w
        else:
            neutral += w

    if bull > bear and bull > neutral:
        dominant = "bullish"
    elif bear > bull and bear > neutral:
        dominant = "bearish"
    else:
        dominant = "mixed"

    total = bull + bear + neutral
    dominant_score = bull if dominant == "bullish" else bear if dominant == "bearish" else neutral
    score = round(dominant_score / total, 3) if total > 0 else 0.0

    for tf, direction in directions.items():
        if dominant in ("bullish", "bearish") and direction == dominant:
            aligned_tfs.append(tf)
        elif direction in ("bullish", "bearish") and dominant in ("bullish", "bearish") and direction != dominant:
            conflict_tfs.append(tf)

    return {
        "direction": dominant,
        "score": score,
        "aligned_tfs": aligned_tfs,
        "conflict_tfs": conflict_tfs,
        "directions_by_tf": directions,
    }


def build_gpt_skinny_snapshot(
    *,
    symbol: str,
    snapshots_by_tf: Dict[str, Dict[str, Any]],
    spot_events_by_tf: Optional[Dict[str, Dict[str, Any]]] = None,
    live_price: Optional[float] = None,
    live_ts: Optional[str] = None,
    decision_ts: Optional[str] = None,
    include_tfs: Optional[List[str]] = None,
) -> Dict[str, Any]:
    include_tfs = include_tfs or DEFAULT_INCLUDE_TFS
    spot_events_by_tf = spot_events_by_tf or {}

    decision = build_decision_price(
        snapshots_by_tf,
        live_price=live_price,
        live_ts=live_ts,
        decision_ts=decision_ts,
    )
    dp = _safe_float(decision.get("price"))

    tfs: Dict[str, Dict[str, Any]] = {}
    for tf in include_tfs:
        snap = snapshots_by_tf.get(tf)
        if not isinstance(snap, dict):
            continue
        tfs[tf] = build_tf_skinny_snapshot(tf=tf, snapshot=snap, event_state=spot_events_by_tf.get(tf) or {}, decision_price=dp)

    return {
        "schema": "gpt_skinny_snapshot_v1",
        "symbol": str(symbol or "").upper(),
        "created_at": decision.get("decision_ts") or _now_utc_iso(),
        "decision": decision,
        "summary": build_snapshot_summary(tfs),
        "tfs": tfs,
    }


def build_gpt_strategy_input(
    *,
    symbol: str,
    snapshots_by_tf: Dict[str, Dict[str, Any]],
    spot_events_by_tf: Optional[Dict[str, Dict[str, Any]]] = None,
    active_setups: Optional[List[Dict[str, Any]]] = None,
    live_price: Optional[float] = None,
    live_ts: Optional[str] = None,
    decision_ts: Optional[str] = None,
    include_tfs: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "market": build_gpt_skinny_snapshot(
            symbol=symbol,
            snapshots_by_tf=snapshots_by_tf,
            spot_events_by_tf=spot_events_by_tf,
            live_price=live_price,
            live_ts=live_ts,
            decision_ts=decision_ts,
            include_tfs=include_tfs,
        ),
        "active_setups": active_setups or [],
    }
