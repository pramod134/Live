# spot_event.py
"""
Spot Events Engine (strategy-agnostic)

Implements finalized V1 events + BOS/CHOCH trackers with AVWAP states.

DB philosophy (per your plan):
- This module is PURE logic (no DB calls).
- indicator_bot provides in-memory context + previous spot_events state from cache.
- indicator_bot does a single DB upsert after this returns.

FINALIZED V1:
events_latest trigger types:
- displacement_up/down
- liquidity_sweep_high/low
- structure_break_up/down   (BOS)
- choch_up/down
- fvg_bullish_created / fvg_bearish_created
- fvg_bullish_tapped / fvg_bearish_tapped
- fvg_bullish_filled / fvg_bearish_filled

events_active:
- BOS + CHOCH trackers only (active + expired per slot)
- No global acceptance/rejection keys
- No time-based timeout
"""

from __future__ import annotations

from dataclasses import dataclass
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
import atexit
import copy
import json
import math
import os
from datetime import datetime, timezone


# -------------------------
# Helpers
# -------------------------

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default

def _is_iso_ts(s: Any) -> bool:
    return isinstance(s, str) and ("T" in s) and (("+" in s) or s.endswith("Z"))

def _hlc3(c: Dict[str, Any]) -> float:
    h = _safe_float(c.get("high"))
    l = _safe_float(c.get("low"))
    cl = _safe_float(c.get("close"))
    return (h + l + cl) / 3.0

def _event_id(event_type: str, ts: str, timeframe: str, extra: str = "") -> str:
    # Deterministic enough for idempotence (per candle).
    base = f"{event_type}:{timeframe}:{ts}"
    return f"{base}:{extra}" if extra else base


DEBUG = os.getenv("SPOT_EVENT_DEBUG", "0").strip().lower() in ("1", "true", "yes", "y", "on")
SPOT_EVENT_FIRST_N = int(os.getenv("SPOT_EVENT_FIRST_N", "3") or "3")
SPOT_EVENT_FVG_SAMPLE = int(os.getenv("SPOT_EVENT_FVG_SAMPLE", "3") or "3")

# Track how many debug blocks we've printed per (symbol, timeframe)
_dbg_seen_counts: Dict[Tuple[str, str], int] = {}

# Debug storage: print exactly what spot_event receives
# once at first call, and once at process exit for the last snapshot of each tf.
_SWINGS_SIM_FIRST_PRINTED = False
_SWINGS_SIM_LAST: Dict[str, Dict[str, Any]] = {}


def _print_swings_sim_end() -> None:
    try:
        # print("[SWINGS_SIM_END]")
        for tf, data in sorted(_SWINGS_SIM_LAST.items()):
            # print("timeframe:", tf)
            # print("ts:", data.get("ts"))
            # print("swing_highs:", data.get("swing_highs"))
            # print("swing_lows:", data.get("swing_lows"))
            pass
    except Exception:
        pass


atexit.register(_print_swings_sim_end)


# ------------------------------------------------------------------
# Diagnostics:
# 1) How many times each detector function is CALLED (per compute_spot_events call)
# 2) How many times each event is TRULY TRIGGERED on that candle
#    (transition from "not triggered" -> "triggered" on this call)
# 3) The candle timestamps (ts) where each event TRIGGERED
# 4) What structure_state values spot_event is receiving (normalized)
# ------------------------------------------------------------------
_SPOT_EVENT_DETECTOR_CALL_COUNTS = defaultdict(int)
_SPOT_EVENT_TRIGGER_COUNTS = defaultdict(int)
_SPOT_EVENT_TRIGGER_TS = defaultdict(list)  # event_key -> list of trigger detail dicts
_SPOT_EVENT_STRUCTURE_STATE_COUNTS = defaultdict(int)
_SPOT_EVENT_STRUCTURE_STATE_COUNTS_BY_TF = defaultdict(int)  # "tf:state" -> count
_SPOT_EVENT_LAST_STRUCTURE_STATE = None
_SPOT_EVENT_CHOCH_BRANCH_SCANS = {"bullish_branch": 0, "bearish_branch": 0}
_SPOT_EVENT_CHOCH_DIAG = {
    "strength_failed": 0,
    "bullish_no_ref": 0,
    "bullish_no_cross": 0,
    "bearish_no_ref": 0,
    "bearish_no_cross": 0,
}

# CHOCH accounting extras (log-only, no behavior change)
_SPOT_EVENT_CHOCH_DIAG_EXTRA = {
    "missing_last_candle": 0,
    "missing_ts": 0,
    "missing_close": 0,
    "close_non_numeric_or_nan": 0,
    "strength_failed_bullish_branch": 0,
    "strength_failed_bearish_branch": 0,
    "bullish_cross_true": 0,
    "bearish_cross_true": 0,
}


def reset_spot_event_counters() -> None:
    _SPOT_EVENT_DETECTOR_CALL_COUNTS.clear()
    _SPOT_EVENT_TRIGGER_COUNTS.clear()
    _SPOT_EVENT_TRIGGER_TS.clear()
    _SPOT_EVENT_STRUCTURE_STATE_COUNTS.clear()
    _SPOT_EVENT_STRUCTURE_STATE_COUNTS_BY_TF.clear()
    _SPOT_EVENT_CHOCH_BRANCH_SCANS["bullish_branch"] = 0
    _SPOT_EVENT_CHOCH_BRANCH_SCANS["bearish_branch"] = 0
    for k in _SPOT_EVENT_CHOCH_DIAG:
        _SPOT_EVENT_CHOCH_DIAG[k] = 0
    for k in _SPOT_EVENT_CHOCH_DIAG_EXTRA:
        _SPOT_EVENT_CHOCH_DIAG_EXTRA[k] = 0

    global _SPOT_EVENT_LAST_STRUCTURE_STATE
    _SPOT_EVENT_LAST_STRUCTURE_STATE = None


def print_spot_event_counters() -> None:
    ordered_calls = dict(sorted(_SPOT_EVENT_DETECTOR_CALL_COUNTS.items(), key=lambda kv: kv[0]))
    ordered_trig = dict(sorted(_SPOT_EVENT_TRIGGER_COUNTS.items(), key=lambda kv: kv[0]))
    # print(f"[SPOT_EVENT][DIAG] detector_call_counts={ordered_calls}")
    # print(f"[SPOT_EVENT][DIAG] triggered_counts={ordered_trig}")
    ordered_ts = dict(sorted(_SPOT_EVENT_TRIGGER_TS.items(), key=lambda kv: kv[0]))
    # print(f"[SPOT_EVENT][DIAG] triggered_ts_by_event={ordered_ts}")
    ordered_states = dict(sorted(_SPOT_EVENT_STRUCTURE_STATE_COUNTS.items(), key=lambda kv: kv[0]))
    # print(f"[SPOT_EVENT][DIAG] structure_state_counts={ordered_states}")
    ordered_states_tf = dict(sorted(_SPOT_EVENT_STRUCTURE_STATE_COUNTS_BY_TF.items(), key=lambda kv: kv[0]))
    # print(f"[SPOT_EVENT][DIAG] structure_state_counts_by_tf={ordered_states_tf}")
    # print(f"[SPOT_EVENT][DIAG] last_structure_state={_SPOT_EVENT_LAST_STRUCTURE_STATE}")
    # print(f"[SPOT_EVENT][DIAG] choch_branch_scans={_SPOT_EVENT_CHOCH_BRANCH_SCANS}")
    # print(f"[SPOT_EVENT][DIAG] choch_diag={_SPOT_EVENT_CHOCH_DIAG}")
    # print(f"[SPOT_EVENT][DIAG] choch_diag_extra={_SPOT_EVENT_CHOCH_DIAG_EXTRA}")


def _is_triggered_value(v: Any) -> bool:
    """
    Decide whether an events_latest value represents a real trigger.
    - None -> not triggered
    - bool -> True triggers
    - dict/list/tuple/set -> non-empty triggers
    - str -> non-empty triggers
    - numbers -> non-zero triggers
    - other -> truthy triggers
    """
    if v is None:
        return False
    if isinstance(v, bool):
        return v is True
    if isinstance(v, (dict, list, tuple, set)):
        return len(v) > 0
    if isinstance(v, str):
        return v.strip() != ""
    if isinstance(v, (int, float)):
        return v != 0
    return bool(v)


def _extract_ref_candle_ts(meta_now: Dict[str, Any]) -> Any:
    """
    Best-effort extraction of the reference candle timestamp from event meta.
    Different detectors use slightly different key names.
    """
    for k in ("ref_ts", "reference_candle_ts", "ref_candle_ts", "anchor_ts", "swing_ts"):
        if meta_now.get(k) is not None:
            return meta_now.get(k)
    return None


def _extract_ref_level(meta_now: Dict[str, Any]) -> Any:
    """
    Best-effort extraction of reference level from event meta.
    """
    for k in ("ref_level", "level", "swing_level", "anchor_level"):
        if meta_now.get(k) is not None:
            return meta_now.get(k)
    return None


def _trigger_signature(v: Any) -> str:
    """
    Stable signature for trigger payloads.

    Why:
    events_latest values are often "sticky" (remain truthy after the first trigger),
    so a plain False->True transition counter only increments once per TF.
    We treat a trigger as *new* when its payload signature changes
    (e.g., newer ts/event_id/meta), while still keeping the first False->True case.
    """
    try:
        return json.dumps(v, sort_keys=True, default=str)
    except Exception:
        return str(v)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_structure_state(structure_state: str) -> str:
    """
    CHOCH detector historically expected exact strings: 'bullish'/'bearish'.
    In practice spot_tf.structure_state can contain richer labels like:
      - bull_trend_HH_HL_bos_up
      - bear_trend_LH_LL_bos_down
      - range_bos_up
    Normalize by substring so CHOCH can actually fire.
    """
    s = (structure_state or "").lower().strip()
    if not s:
        return ""
    # Map richer "range BOS" labels to directional bias for CHOCH gating.
    if "range_bos_up" in s:
        return "bullish"
    if "range_bos_down" in s:
        return "bearish"
    if "bear" in s:
        return "bearish"
    if "bull" in s:
        return "bullish"
    return s


def _ref_is_already_broken(ref_raw: Dict[str, Any], ts: str) -> bool:
    """
    Prevent repeated BOS on the same ref level after it has already broken.
    Supports either:
      - ref_raw itself has state/broken_ts (swing object), or
      - ref_raw contains a nested 'swing' dict (as in structural points inheritance).
    Rule:
      - If state=='broken' AND broken_ts exists AND broken_ts < ts => already broken.
      - If broken_ts == ts => allow (this candle is the break candle).
    """
    if not isinstance(ref_raw, dict) or not _is_iso_ts(ts):
        return False
    swing = ref_raw.get("swing")
    obj = swing if isinstance(swing, dict) else ref_raw
    state = (obj.get("state") or "").lower()
    broken_ts = obj.get("broken_ts")
    return state == "broken" and _is_iso_ts(broken_ts) and broken_ts < ts

def _append_recent_unique(events_recent: List[Dict[str, Any]], ev: Dict[str, Any], max_scan: int = 50) -> None:
    """
    Idempotence: avoid duplicate append if called twice for same candle.
    We scan only the last max_scan events for speed.
    """
    ev_id = ev.get("event_id")
    if not ev_id:
        return
    for prior in reversed(events_recent[-max_scan:]):
        if prior.get("event_id") == ev_id:
            return
    events_recent.append(ev)

def _parse_levels(levels_any: Any) -> List[Dict[str, Any]]:
    """
    spot_liquidity_pool.levels might be stored as JSON string (as in your sample),
    or as already-parsed list.
    """
    if levels_any is None:
        return []
    if isinstance(levels_any, list):
        return [x for x in levels_any if isinstance(x, dict)]
    if isinstance(levels_any, str):
        s = levels_any.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [x for x in parsed if isinstance(x, dict)]
        except Exception:
            return []
    return []

def _normalize_level(level: Dict[str, Any], tol_price: float) -> Dict[str, Any]:
    """
    Normalize liquidity pool level to a consistent shape:
    - id
    - kind: 'high' or 'low'
    - level (center)
    - band_low/band_high using tol_price
    - strength, source_tf, source_type, ts_ref
    """
    lvl = _safe_float(level.get("level", level.get("price", 0.0)))
    return {
        "id": level.get("id") or "",
        "kind": level.get("kind") or level.get("side") or "",
        "level": lvl,
        "band_low": _safe_float(level.get("band_low", lvl - tol_price)),
        "band_high": _safe_float(level.get("band_high", lvl + tol_price)),
        "strength": _safe_float(level.get("strength", 0.0)),
        "source_tf": level.get("source_tf"),
        "source_type": level.get("source_type"),
        "ts_ref": level.get("ts_ref"),
        "sources": level.get("sources"),
    }

def _pick_most_recent_ref(levels: List[Dict[str, Any]], ts: str) -> Optional[Dict[str, Any]]:
    """
    levels: list of dicts with at least ts_ref.
    Returns most recent level whose ts_ref < ts. If ts_ref missing, keep as last-resort.
    """
    if not levels:
        return None
    dated = []
    undated = []
    for x in levels:
        t = x.get("ts_ref")
        if _is_iso_ts(t) and _is_iso_ts(ts) and t < ts:
            dated.append(x)
        else:
            undated.append(x)
    if dated:
        dated.sort(key=lambda d: d.get("ts_ref"))
        return dated[-1]
    return undated[-1] if undated else None

def _pick_best_liquidity_match(
    candidates: List[Dict[str, Any]],
    last_candle: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    If multiple pools match the sweep condition, pick deterministically:
    1) highest strength
    2) higher timeframe preference (rough heuristic)
    3) closest to candle extreme
    """
    if not candidates:
        return None

    tf_rank = {
        "1m": 1, "3m": 2, "5m": 3, "15m": 4, "30m": 5,
        "1h": 6, "2h": 7, "4h": 8, "1d": 9, "1w": 10
    }
    high = _safe_float(last_candle.get("high"))
    low = _safe_float(last_candle.get("low"))

    def key_fn(x: Dict[str, Any]) -> Tuple[float, int, float]:
        strength = _safe_float(x.get("strength"))
        tf = (x.get("source_tf") or "").lower()
        rank = tf_rank.get(tf, 0)
        kind = (x.get("kind") or "").lower()
        lvl = _safe_float(x.get("level"))
        dist = abs((high - lvl) if kind == "high" else (low - lvl))
        # Sort descending strength, descending rank, ascending dist
        return (strength, rank, -dist)

    candidates.sort(key=key_fn, reverse=True)
    return candidates[0]


# -------------------------
# Defaults / Initializers
# -------------------------

LATEST_KEYS = [
    "displacement_up", "displacement_down",
    "liquidity_sweep_high", "liquidity_sweep_low",
    "structure_break_up", "structure_break_down",
    "choch_up", "choch_down",
    # FVG (transition-based)
    "fvg_bullish_created", "fvg_bearish_created",
    "fvg_bullish_tapped", "fvg_bearish_tapped",
    "fvg_bullish_filled", "fvg_bearish_filled",
]

TRACKER_SLOTS = [
    "bos_bull_tracker",
    "bos_bear_tracker",
    "choch_bull_tracker",
    "choch_bear_tracker",
]

def init_events_latest() -> Dict[str, Any]:
    return {k: None for k in LATEST_KEYS}

def init_events_active() -> Dict[str, Any]:
    # Each slot stores max 1 active + 1 expired
    return {slot: {"active": None, "expired": None} for slot in TRACKER_SLOTS}

def init_events_recent() -> List[Dict[str, Any]]:
    return []


# -------------------------
# Event Builders
# -------------------------

def _make_event(event_type: str, ts: str, timeframe: str, score: float, meta: Dict[str, Any], extra_id: str = "") -> Dict[str, Any]:
    ev = {
        "type": event_type,
        "ts": ts,
        "timeframe": timeframe,
        "score": float(max(0.0, min(100.0, score))),
        "meta": meta or {},
    }
    ev["event_id"] = _event_id(event_type, ts, timeframe, extra_id)
    return ev


# -------------------------
# Detection: Displacement (finalized)
# -------------------------

def detect_displacement(last_candle: Dict[str, Any]) -> Optional[Tuple[str, float, Dict[str, Any]]]:
    direction = (last_candle.get("direction") or "").lower()  # "bull" / "bear"
    mom_atr = _safe_float(last_candle.get("mom_atr"))
    mom_atr_mag = abs(mom_atr)
    vol_rel = _safe_float(last_candle.get("vol_rel"))
    spread_strength = _safe_float(last_candle.get("spread_strength"))
    cluster = last_candle.get("cluster") or {}
    cluster_state = (cluster.get("state") or "").lower()

    if direction not in ("bull", "bear"):
        return None

    mom_req = 1.00
    if cluster_state == "chop":
        mom_req = 1.15

    # mom_atr is signed (negative on down moves), but displacement strength
    # gating should be based on magnitude for both bull and bear impulses.
    if mom_atr_mag < mom_req:
        return None
    if spread_strength < 0.60:
        return None

    event_type = "displacement_up" if direction == "bull" else "displacement_down"

    # Simple score shaping (deterministic, not critical)
    score = 60.0
    score += min(20.0, (mom_atr_mag - mom_req) * 20.0)
    score += min(10.0, max(0.0, vol_rel - 1.5) * 3.0)
    score += min(10.0, max(0.0, spread_strength - 0.60) * 50.0)

    meta = {
        "mom_atr": mom_atr,
        "mom_atr_mag": mom_atr_mag,
        "vol_rel": vol_rel,
        "spread_strength": spread_strength,
        "body": _safe_float(last_candle.get("body")),
        "range": _safe_float(last_candle.get("range")),
        "upper_wick": _safe_float(last_candle.get("upper_wick")),
        "lower_wick": _safe_float(last_candle.get("lower_wick")),
        "cluster_state": cluster_state,
    }
    return event_type, score, meta


# -------------------------
# Detection: Liquidity Sweep (finalized)
# -------------------------

def detect_liquidity_sweep(
    last_candle: Dict[str, Any],
    liquidity_pool_row: Optional[Dict[str, Any]],
    prev_recent: List[Dict[str, Any]],
    timeframe: str,
) -> Optional[Tuple[str, float, Dict[str, Any], str]]:
    """
    Returns: (event_type, score, meta, level_id_for_event_id_extra)
    Cooldown: do not re-fire same level_id within 3 bars => enforced by scanning recent.
    """
    if not liquidity_pool_row:
        return None

    tol_price = _safe_float(liquidity_pool_row.get("tol_price"), 0.0)
    levels = _parse_levels(liquidity_pool_row.get("levels"))
    if not levels:
        return None

    # Normalize and filter eligible levels
    norm = [_normalize_level(x, tol_price) for x in levels]
    eligible = [x for x in norm if _safe_float(x.get("strength")) >= 60.0 and (x.get("kind") in ("high", "low"))]
    if not eligible:
        return None

    ts = last_candle.get("ts")
    high = _safe_float(last_candle.get("high"))
    low = _safe_float(last_candle.get("low"))
    close = _safe_float(last_candle.get("close"))
    open_ = _safe_float(last_candle.get("open"))
    rng = max(1e-9, _safe_float(last_candle.get("range")))
    upper_wick = _safe_float(last_candle.get("upper_wick"))
    lower_wick = _safe_float(last_candle.get("lower_wick"))
    spread_strength = _safe_float(last_candle.get("spread_strength"))
    mom_atr = _safe_float(last_candle.get("mom_atr"))
    vol_rel = _safe_float(last_candle.get("vol_rel"))
    is_high_vol = bool(last_candle.get("is_high_vol", False))

    # Candidate detection
    sweep_high_candidates = []
    sweep_low_candidates = []

    for lvl in eligible:
        kind = lvl["kind"]
        band_low = lvl["band_low"]
        band_high = lvl["band_high"]

        if kind == "high":
            # Sweep High condition:
            # 1) high > band_high
            # 2) close < band_low
            if high > band_high and close < band_low:
                # rejection quality: at least one
                rej_ok = (
                    (upper_wick >= 0.40 * rng) or
                    (spread_strength <= 0.40) or
                    (close < open_)
                )
                # participation: at least one
                part_ok = (
                    (vol_rel >= 1.5) or
                    (mom_atr >= 0.9) or
                    is_high_vol
                )
                if rej_ok and part_ok:
                    sweep_high_candidates.append(lvl)

        if kind == "low":
            # Sweep Low condition:
            # 1) low < band_low
            # 2) close > band_high
            if low < band_low and close > band_high:
                rej_ok = (
                    (lower_wick >= 0.40 * rng) or
                    (spread_strength >= 0.60) or
                    (close > open_)
                )
                part_ok = (
                    (vol_rel >= 1.5) or
                    (mom_atr >= 0.9) or
                    is_high_vol
                )
                if rej_ok and part_ok:
                    sweep_low_candidates.append(lvl)

    # Cooldown by level_id: do not re-fire same id within last 3 events of same type.
    def in_cooldown(level_id: str, event_type: str) -> bool:
        # Scan last ~30 for safety (cheap)
        count = 0
        for ev in reversed(prev_recent[-50:]):
            if ev.get("type") == event_type:
                if ev.get("meta", {}).get("level_id") == level_id:
                    return True
                count += 1
                if count >= 3:
                    break
        return False

    # Choose best
    if sweep_high_candidates:
        best = _pick_best_liquidity_match(sweep_high_candidates, last_candle)
        if best and not in_cooldown(best["id"], "liquidity_sweep_high"):
            raid_depth = max(0.0, high - best["band_high"])
            reclaim_depth = max(0.0, best["band_low"] - close)
            score = min(100.0, 65.0 + best["strength"] * 0.2 + min(10.0, raid_depth * 5.0) + min(10.0, reclaim_depth * 5.0))
            meta = {
                "level_id": best["id"],
                "level_center": best["level"],
                "band_low": best["band_low"],
                "band_high": best["band_high"],
                "strength": best["strength"],
                "source_tf": best.get("source_tf"),
                "source_type": best.get("source_type"),
                "ts_ref": best.get("ts_ref"),
                "sources": best.get("sources"),
                "raid_depth": raid_depth,
                "reclaim_depth": reclaim_depth,
                "mom_atr": mom_atr,
                "vol_rel": vol_rel,
                "spread_strength": spread_strength,
            }
            return "liquidity_sweep_high", score, meta, best["id"]

    if sweep_low_candidates:
        best = _pick_best_liquidity_match(sweep_low_candidates, last_candle)
        if best and not in_cooldown(best["id"], "liquidity_sweep_low"):
            raid_depth = max(0.0, best["band_low"] - low)
            reclaim_depth = max(0.0, close - best["band_high"])
            score = min(100.0, 65.0 + best["strength"] * 0.2 + min(10.0, raid_depth * 5.0) + min(10.0, reclaim_depth * 5.0))
            meta = {
                "level_id": best["id"],
                "level_center": best["level"],
                "band_low": best["band_low"],
                "band_high": best["band_high"],
                "strength": best["strength"],
                "source_tf": best.get("source_tf"),
                "source_type": best.get("source_type"),
                "ts_ref": best.get("ts_ref"),
                "sources": best.get("sources"),
                "raid_depth": raid_depth,
                "reclaim_depth": reclaim_depth,
                "mom_atr": mom_atr,
                "vol_rel": vol_rel,
                "spread_strength": spread_strength,
            }
            return "liquidity_sweep_low", score, meta, best["id"]

    return None


# -------------------------
# Detection: BOS / CHOCH triggers (finalized)
# -------------------------

def _get_ref_level(ref_list: List[Dict[str, Any]], ts: str) -> Optional[Dict[str, Any]]:
    """
    Expects elements with keys: ts (or ts_ref) and price/level.
    Returns most recent before ts.
    """
    if not ref_list:
        return None
    # Normalize
    norm = []
    for x in ref_list:
        t = x.get("ts") or x.get("ts_ref")
        p = x.get("price", x.get("level"))
        if _is_iso_ts(t) and p is not None:
            norm.append({"ts": t, "level": _safe_float(p), "raw": x})
    if not norm:
        return None
    norm = [x for x in norm if x["ts"] < ts]
    if not norm:
        return None
    norm.sort(key=lambda d: d["ts"])
    return norm[-1]

def detect_bos(
    last_candle: Dict[str, Any],
    structural_highs: List[Dict[str, Any]],
    structural_lows: List[Dict[str, Any]],
    displacement_fired: bool,
) -> Optional[Tuple[str, float, Dict[str, Any]]]:
    ts = last_candle.get("ts")
    close = _safe_float(last_candle.get("close"))
    mom_atr = _safe_float(last_candle.get("mom_atr"))
    vol_rel = _safe_float(last_candle.get("vol_rel"))
    spread_strength = _safe_float(last_candle.get("spread_strength"))

    hi_ref = _get_ref_level(structural_highs, ts)
    lo_ref = _get_ref_level(structural_lows, ts)

    # If the ref level is already marked broken prior to this candle, skip it.
    # This avoids repeated BOS triggers on the same structural ref every candle.
    if hi_ref and _ref_is_already_broken(hi_ref.get("raw") or {}, ts):
        hi_ref = None
    if lo_ref and _ref_is_already_broken(lo_ref.get("raw") or {}, ts):
        lo_ref = None

    # Strength filter: displacement OR mom_atr>=0.9 OR vol_rel>=1.5
    strength_ok = displacement_fired or (mom_atr >= 0.9) or (vol_rel >= 1.5)
    if not strength_ok:
        return None

    if hi_ref and close > hi_ref["level"]:
        dist = close - hi_ref["level"]
        score = min(100.0, 70.0 + min(15.0, dist * 10.0) + min(10.0, max(0.0, vol_rel - 1.5) * 4.0))
        meta = {
            "ref_type": "structural",
            "ref_level": hi_ref["level"],
            "ref_ts": hi_ref["ts"],
            "break_close": close,
            "break_distance": dist,
            "mom_atr": mom_atr,
            "vol_rel": vol_rel,
            "spread_strength": spread_strength,
        }
        return "structure_break_up", score, meta

    if lo_ref and close < lo_ref["level"]:
        dist = lo_ref["level"] - close
        score = min(100.0, 70.0 + min(15.0, dist * 10.0) + min(10.0, max(0.0, vol_rel - 1.5) * 4.0))
        meta = {
            "ref_type": "structural",
            "ref_level": lo_ref["level"],
            "ref_ts": lo_ref["ts"],
            "break_close": close,
            "break_distance": dist,
            "mom_atr": mom_atr,
            "vol_rel": vol_rel,
            "spread_strength": spread_strength,
        }
        return "structure_break_down", score, meta

    return None

def detect_choch(
    last_candle: Dict[str, Any],
    swing_highs: List[Dict[str, Any]],
    swing_lows: List[Dict[str, Any]],
    structure_state: str,
) -> Optional[Tuple[str, float, Dict[str, Any]]]:
    # --- DEBUG: print latest swing refs being used ---
    try:
        last_highs = swing_highs[-3:]
        last_lows = swing_lows[-3:]

        # print(
        # "[CHOCH_SWINGS] highs="
        # + str([(x.get("price") or x.get("level"), x.get("ts") or x.get("ts_ref")) for x in last_highs])
        # )

        # print(
        # "[CHOCH_SWINGS] lows="
        # + str([(x.get("price") or x.get("level"), x.get("ts") or x.get("ts_ref")) for x in last_lows])
        # )
    except Exception:
        pass

    if not last_candle:
        _SPOT_EVENT_CHOCH_DIAG_EXTRA["missing_last_candle"] += 1
        return None

    ts = last_candle.get("ts")
    if ts is None:
        _SPOT_EVENT_CHOCH_DIAG_EXTRA["missing_ts"] += 1

    close_raw = last_candle.get("close")
    if close_raw is None:
        _SPOT_EVENT_CHOCH_DIAG_EXTRA["missing_close"] += 1

    # Track non-numeric / NaN close (log-only). Do NOT alter logic.
    if close_raw is not None:
        try:
            close_num = float(close_raw)
            # NaN check: NaN != NaN
            if close_num != close_num:
                _SPOT_EVENT_CHOCH_DIAG_EXTRA["close_non_numeric_or_nan"] += 1
        except Exception:
            _SPOT_EVENT_CHOCH_DIAG_EXTRA["close_non_numeric_or_nan"] += 1

    close = _safe_float(last_candle.get("close"))
    mom_atr = _safe_float(last_candle.get("mom_atr"))
    vol_rel = _safe_float(last_candle.get("vol_rel"))
    spread_strength = _safe_float(last_candle.get("spread_strength"))
    structure_state = _normalize_structure_state(structure_state)

    # Count branch scans immediately after normalization.
    if structure_state == "bearish":
        _SPOT_EVENT_CHOCH_BRANCH_SCANS["bearish_branch"] += 1
    elif structure_state == "bullish":
        _SPOT_EVENT_CHOCH_BRANCH_SCANS["bullish_branch"] += 1

    # Strength filter: mom_atr>=0.8 OR vol_rel>=1.2
    # DEBUG: temporarily disable strength filter
    # strength_ok = (mom_atr >= 0.8) or (vol_rel >= 1.2)
    strength_ok = True
    if not strength_ok:
        _SPOT_EVENT_CHOCH_DIAG["strength_failed"] += 1
        if structure_state == "bullish":
            _SPOT_EVENT_CHOCH_DIAG_EXTRA["strength_failed_bullish_branch"] += 1
        elif structure_state == "bearish":
            _SPOT_EVENT_CHOCH_DIAG_EXTRA["strength_failed_bearish_branch"] += 1
        return None

    hi_ref = _get_ref_level(swing_highs, ts)
    lo_ref = _get_ref_level(swing_lows, ts)

    # ------------------------------------------------------------
    # DEBUG (5m only): reference + swing diagnostics
    # ------------------------------------------------------------
    candle_tf = last_candle.get("timeframe") or last_candle.get("tf")

    if candle_tf == "5m":
        try:
            current_ref_high = hi_ref.get("level") if hi_ref else None
            current_ref_low = lo_ref.get("level") if lo_ref else None

            latest_swing_high = swing_highs[-1].get("level") if swing_highs else None
            latest_swing_low = swing_lows[-1].get("level") if swing_lows else None

            last3_highs = [x.get("level") for x in swing_highs[-3:]] if swing_highs else []
            last3_lows = [x.get("level") for x in swing_lows[-3:]] if swing_lows else []

            # print(
            #     f"[5M_SWING_DEBUG] "
            #     f"ts={ts} "
            #     f"CURRENT_REF_HIGH={current_ref_high} "
            #     f"CURRENT_REF_LOW={current_ref_low} "
            #     f"LATEST_SWING_HIGH={latest_swing_high} "
            #     f"LATEST_SWING_LOW={latest_swing_low} "
            #     f"LAST_3_SWING_HIGHS={last3_highs} "
            #     f"LAST_3_SWING_LOWS={last3_lows}"
            # )
        except Exception:
            pass

    # ------------------------------------------------------------
    # LOG ONLY: CHOCH diagnostic print
    # ------------------------------------------------------------
    candle_high = last_candle.get("high")
    candle_low = last_candle.get("low")
    ref_level = None
    break_condition = None
    close_minus_ref = None

    if structure_state == "bearish":
        if hi_ref:
            ref_level = hi_ref.get("price") or hi_ref.get("level")
        if close is not None and ref_level is not None:
            break_condition = close > ref_level
            close_minus_ref = close - ref_level
    elif structure_state == "bullish":
        if lo_ref:
            ref_level = lo_ref.get("price") or lo_ref.get("level")
        if close is not None and ref_level is not None:
            break_condition = close < ref_level
            close_minus_ref = close - ref_level

    # print(
    #     f"ts={ts}, tf={candle_tf}, state={structure_state}, "
    #     f"h={candle_high}, l={candle_low}, c={close}, "
    #     f"ref={ref_level}, break={break_condition}, c-ref={close_minus_ref}"
    # )

    ref = lo_ref
    if ref:
        ref_price = ref.get("price") or ref.get("level")
        # print(
        # f"[CHOCH_REF_INTERNAL] ts={ts} "
        # f"close={close} "
        # f"ref_price_used={ref_price} "
        # f"ref_ts_used={ref.get('ts')}"
        # )

    if structure_state == "bearish":
        if hi_ref:
            candle_tf = last_candle.get("timeframe") or last_candle.get("tf")
            swing_tf = hi_ref.get("timeframe") or hi_ref.get("tf")
            # print(
            # f"[CHOCH_CHECK][UP] "
            # f"candle_tf={candle_tf} swing_tf={swing_tf} "
            # f"candle_ts={ts} ref_ts={hi_ref.get('ts') or hi_ref.get('ts_ref')} "
            # f"close={close} ref_level={hi_ref.get('price') or hi_ref.get('level')}"
            # )
            # print(
            # f"[CHOCH_COMPARE_UP] "
            # f"ts={ts} "
            # f"state={structure_state} "
            # f"close={close} "
            # f"ref_used={hi_ref.get('price') or hi_ref.get('level')} "
            # f"ref_ts={hi_ref.get('ts')}"
            # )
        if not hi_ref:
            _SPOT_EVENT_CHOCH_DIAG["bearish_no_ref"] += 1
        elif close > hi_ref["level"]:
            _SPOT_EVENT_CHOCH_DIAG_EXTRA["bearish_cross_true"] += 1
        elif close <= hi_ref["level"]:
            _SPOT_EVENT_CHOCH_DIAG["bearish_no_cross"] += 1

    # CHOCH up only if currently bearish
    if structure_state == "bearish" and hi_ref and close > hi_ref["level"]:
        dist = close - hi_ref["level"]
        score = min(100.0, 65.0 + min(20.0, dist * 10.0) + min(10.0, max(0.0, vol_rel - 1.2) * 5.0))
        meta = {
            "ref_type": "swing",
            "ref_level": hi_ref["level"],
            "ref_ts": hi_ref["ts"],
            "structure_state": structure_state,
            "break_close": close,
            "break_distance": dist,
            "mom_atr": mom_atr,
            "vol_rel": vol_rel,
            "spread_strength": spread_strength,
        }
        return "choch_up", score, meta

    if structure_state == "bullish":
        if lo_ref:
            candle_tf = last_candle.get("timeframe") or last_candle.get("tf")
            high = last_candle.get("high")
            low = last_candle.get("low")
            close = last_candle.get("close")
            # print(
            # f"[CHOCH_CHECK][DOWN] "
            # f"tf={candle_tf} "
            # f"state={structure_state} "
            # f"candle_ts={ts} "
            # f"H={high} L={low} C={close} "
            # f"ref_level={lo_ref.get('price') or lo_ref.get('level')} "
            # f"ref_ts={lo_ref.get('ts') or lo_ref.get('ts_ref')}"
            # )
            # print(
            # f"[CHOCH_COMPARE_DOWN] "
            # f"ts={ts} "
            # f"state={structure_state} "
            # f"close={close} "
            # f"ref_used={lo_ref.get('price') or lo_ref.get('level')} "
            # f"ref_ts={lo_ref.get('ts')}"
            # )
        if not lo_ref:
            _SPOT_EVENT_CHOCH_DIAG["bullish_no_ref"] += 1
        elif close < lo_ref["level"]:
            _SPOT_EVENT_CHOCH_DIAG_EXTRA["bullish_cross_true"] += 1
        elif close >= lo_ref["level"]:
            _SPOT_EVENT_CHOCH_DIAG["bullish_no_cross"] += 1

    # CHOCH down only if currently bullish
    if structure_state == "bullish" and lo_ref and close < lo_ref["level"]:
        dist = lo_ref["level"] - close
        score = min(100.0, 65.0 + min(20.0, dist * 10.0) + min(10.0, max(0.0, vol_rel - 1.2) * 5.0))
        meta = {
            "ref_type": "swing",
            "ref_level": lo_ref["level"],
            "ref_ts": lo_ref["ts"],
            "structure_state": structure_state,
            "break_close": close,
            "break_distance": dist,
            "mom_atr": mom_atr,
            "vol_rel": vol_rel,
            "spread_strength": spread_strength,
        }
        return "choch_down", score, meta

    return None


# -------------------------
# Trackers (BOS/CHOCH) with AVWAP + independent sub-states (locked)
# -------------------------

def _new_tracker(
    tracker_type: str,
    anchor_event: str,
    anchor_ts: str,
    timeframe: str,
    ref_type: str,
    ref_level: float,
    ref_ts: str,
    seed_candle: Dict[str, Any],
) -> Dict[str, Any]:
    pv = _hlc3(seed_candle) * _safe_float(seed_candle.get("volume"))
    vv = _safe_float(seed_candle.get("volume"))

    return {
        "type": tracker_type,
        "anchor_event": anchor_event,
        "anchor_ts": anchor_ts,
        "timeframe": timeframe,
        "ref_type": ref_type,
        "ref_level": float(ref_level),
        "ref_ts": ref_ts,
        "bars": 0,
        "last_updated_ts": anchor_ts,
        "structure_state": "pending",
        "vwap_state": "pending",
        "confirm": {
            "structure_closes": 0,
            "vwap_closes": 0,
            "vwap_wrong_side_closes": 0,
        },
        "avwap": {
            "price_mode": "hlc3",
            "anchor_ts": anchor_ts,
            "cum_pv": float(pv),
            "cum_v": float(vv),
            "vwap": float(pv / vv) if vv > 0 else 0.0,
            "last_vwap": float(pv / vv) if vv > 0 else 0.0,
        },
        # lifecycle status (per your updated preference)
        "status": "active",
        "expired_ts": None,
        "expired_reason": None,
        "meta": {},
    }

def _expire_tracker(tracker: Dict[str, Any], ts: str, reason: str) -> Dict[str, Any]:
    t = copy.deepcopy(tracker)
    t["status"] = "expired"
    t["expired_ts"] = ts
    t["expired_reason"] = reason
    return t

def _update_tracker_states(
    tracker: Dict[str, Any],
    last_candle: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Locked defaults:
    - AVWAP uses HLC3
    - BOS structure accepted after 2 closes; CHOCH structure accepted after 3 closes
    - Structure rejected after 1 close back through ref
    - VWAP accepted after 2 closes correct side
    - VWAP rejected after 2 closes wrong side (never 1)
    - No timeout
    """
    t = copy.deepcopy(tracker)
    if t.get("status") != "active":
        return t

    ts = last_candle.get("ts")
    close = _safe_float(last_candle.get("close"))
    volume = _safe_float(last_candle.get("volume"))
    ref_level = _safe_float(t.get("ref_level"))

    # Update AVWAP
    av = t.get("avwap") or {}
    last_vwap = _safe_float(av.get("vwap"))
    pv_add = _hlc3(last_candle) * volume
    cum_pv = _safe_float(av.get("cum_pv")) + pv_add
    cum_v = _safe_float(av.get("cum_v")) + volume
    vwap = (cum_pv / cum_v) if cum_v > 0 else last_vwap

    av["cum_pv"] = float(cum_pv)
    av["cum_v"] = float(cum_v)
    av["last_vwap"] = float(last_vwap)
    av["vwap"] = float(vwap)
    t["avwap"] = av

    # Bars
    t["bars"] = _safe_int(t.get("bars")) + 1
    t["last_updated_ts"] = ts

    # Determine tracker family acceptance counts
    tracker_type = (t.get("type") or "").lower()
    is_bos = tracker_type.startswith("bos_")
    n_structure = 2 if is_bos else 3  # CHOCH = 3
    n_vwap = 2
    k_vwap_reject = 2

    # Bull/bear
    is_bull = ("bull" in tracker_type)

    confirm = t.get("confirm") or {"structure_closes": 0, "vwap_closes": 0, "vwap_wrong_side_closes": 0}
    s_closes = _safe_int(confirm.get("structure_closes"))
    v_closes = _safe_int(confirm.get("vwap_closes"))
    vw_wrong = _safe_int(confirm.get("vwap_wrong_side_closes"))

    # ---- Structure sub-state ----
    if is_bull:
        if close > ref_level:
            s_closes += 1
        else:
            s_closes = 0

        if close < ref_level:
            t["structure_state"] = "rejected"
        elif s_closes >= n_structure:
            t["structure_state"] = "accepted"
        else:
            t["structure_state"] = "pending"

    else:
        if close < ref_level:
            s_closes += 1
        else:
            s_closes = 0

        if close > ref_level:
            t["structure_state"] = "rejected"
        elif s_closes >= n_structure:
            t["structure_state"] = "accepted"
        else:
            t["structure_state"] = "pending"

    # ---- VWAP sub-state ----
    if is_bull:
        if close > vwap:
            v_closes += 1
            vw_wrong = 0
        else:
            v_closes = 0
            vw_wrong += 1

        if v_closes >= n_vwap:
            t["vwap_state"] = "accepted"
        elif vw_wrong >= k_vwap_reject:
            t["vwap_state"] = "rejected"
        else:
            t["vwap_state"] = "pending"

    else:
        if close < vwap:
            v_closes += 1
            vw_wrong = 0
        else:
            v_closes = 0
            vw_wrong += 1

        if v_closes >= n_vwap:
            t["vwap_state"] = "accepted"
        elif vw_wrong >= k_vwap_reject:
            t["vwap_state"] = "rejected"
        else:
            t["vwap_state"] = "pending"

    confirm["structure_closes"] = s_closes
    confirm["vwap_closes"] = v_closes
    confirm["vwap_wrong_side_closes"] = vw_wrong
    t["confirm"] = confirm

    return t

def _slot_set_new_active(
    slot_obj: Dict[str, Any],
    new_tracker: Dict[str, Any],
    expire_ts: str,
    expire_reason: str = "refresh_same_direction",
) -> Dict[str, Any]:
    """
    Per your finalized rule:
    - if slot has active: move it to expired (overwrite expired)
    - start new active
    - keep max 1 expired
    """
    out = copy.deepcopy(slot_obj)
    cur_active = out.get("active")
    if cur_active:
        out["expired"] = _expire_tracker(cur_active, expire_ts, expire_reason)
    out["active"] = new_tracker
    return out


# -------------------------
# FVG events (transition-based, using spot_tf.fvgs)
# -------------------------

def _fvg_sig(f: Dict[str, Any]) -> str:
    # Deterministic signature even if no id field exists
    direction = f.get("direction") or ""
    created_ts = f.get("created_ts") or ""
    low = f.get("low")
    high = f.get("high")
    return f"{direction}:{created_ts}:{low}:{high}"

def detect_fvg_transitions(
    fvgs_now: List[Dict[str, Any]],
    fvgs_prev: Optional[List[Dict[str, Any]]],
    last_candle_ts: str,
    timeframe: str,
) -> List[Dict[str, Any]]:
    """
    Transition-based FVG events ONLY when the FVG's own timestamp equals last_candle_ts.

    LOCKED RULES:
    - created fires only when fvg.created_ts == last_candle_ts
    - tapped fires only when fvg.first_touch_ts == last_candle_ts AND previously untouched
    - filled fires only when fvg.filled_ts == last_candle_ts

    IMPORTANT:
    - If fvgs_prev is None (first run / cache not seeded), emit nothing.
    - Events store reference-only meta (no full context duplication).
    """
    if fvgs_prev is None:
        return []

    prev_map = {_fvg_sig(f): f for f in (fvgs_prev or [])}
    out_events: List[Dict[str, Any]] = []

    for f in fvgs_now or []:
        sig = _fvg_sig(f)
        direction = (f.get("direction") or "").lower()
        if direction not in ("bull", "bear"):
            continue

        prev = prev_map.get(sig) or {}

        created_ts = f.get("created_ts")
        first_touch_ts = f.get("first_touch_ts")
        filled_ts = f.get("filled_ts")

        # Score: prefer trade_score; fallback to fvg_score; fallback to style.confidence; fallback 50
        score = _safe_float(f.get("trade_score"), 0.0)
        if score <= 0.0:
            score = _safe_float(f.get("fvg_score"), 0.0)
        if score <= 0.0:
            style = f.get("style") or {}
            score = _safe_float(style.get("confidence"), 50.0)
        if score <= 0.0:
            score = 50.0

        # ---------- CREATED (timestamp-gated) ----------
        if _is_iso_ts(created_ts) and created_ts == last_candle_ts:
            et = "fvg_bullish_created" if direction == "bull" else "fvg_bearish_created"
            meta = {
                "fvg_sig": sig,
                "direction": direction,
                "low": _safe_float(f.get("low")),
                "high": _safe_float(f.get("high")),
                "created_ts": created_ts,
                "impulse_ts": f.get("impulse_ts"),
                "score_status": f.get("score_status"),
            }
            out_events.append(_make_event(et, last_candle_ts, timeframe, score, meta, extra_id=sig))

        # ---------- TAPPED (timestamp-gated + previously untouched) ----------
        prev_first_touch = prev.get("first_touch_ts")
        if (
            prev_first_touch is None
            and _is_iso_ts(first_touch_ts)
            and first_touch_ts == last_candle_ts
        ):
            et = "fvg_bullish_tapped" if direction == "bull" else "fvg_bearish_tapped"
            meta = {
                "fvg_sig": sig,
                "direction": direction,
                "low": _safe_float(f.get("low")),
                "high": _safe_float(f.get("high")),
                "first_touch_ts": first_touch_ts,
                "touch_count": _safe_int(f.get("touch_count")),
            }
            out_events.append(_make_event(et, last_candle_ts, timeframe, score, meta, extra_id=sig))

        # ---------- FILLED (timestamp-gated) ----------
        if _is_iso_ts(filled_ts) and filled_ts == last_candle_ts:
            et = "fvg_bullish_filled" if direction == "bull" else "fvg_bearish_filled"
            meta = {
                "fvg_sig": sig,
                "direction": direction,
                "low": _safe_float(f.get("low")),
                "high": _safe_float(f.get("high")),
                "filled_ts": filled_ts,
                "filled_pct": _safe_float(f.get("filled_pct")),
            }
            out_events.append(_make_event(et, last_candle_ts, timeframe, score, meta, extra_id=sig))

    return out_events


# -------------------------
# Main Engine
# -------------------------

@dataclass
class SpotEventContext:
    symbol: str
    timeframe: str
    last_candle: Dict[str, Any]

    # Inputs for triggers
    liquidity_pool_row: Optional[Dict[str, Any]] = None  # row from spot_liquidity_pool for this symbol (already loaded/cached)
    structural_highs: Optional[List[Dict[str, Any]]] = None
    structural_lows: Optional[List[Dict[str, Any]]] = None
    swing_highs: Optional[List[Dict[str, Any]]] = None
    swing_lows: Optional[List[Dict[str, Any]]] = None
    structure_state: str = ""

    # FVG
    fvgs_now: Optional[List[Dict[str, Any]]] = None
    fvgs_prev: Optional[List[Dict[str, Any]]] = None  # from cache (previous cycle)

    # Previous spot_events state (from cache)
    prev_events_latest: Optional[Dict[str, Any]] = None
    prev_events_active: Optional[Dict[str, Any]] = None
    prev_events_recent: Optional[List[Dict[str, Any]]] = None


def compute_spot_events(ctx: SpotEventContext) -> Dict[str, Any]:
    """
    Returns:
      {
        "events_latest": dict,
        "events_active": dict,
        "events_recent": list,
        "changed": bool
      }
    """
    global _SWINGS_SIM_FIRST_PRINTED, _SWINGS_SIM_LAST

    symbol = ctx.symbol
    timeframe = ctx.timeframe or ""
    last_candle = ctx.last_candle or {}
    ts = last_candle.get("ts")

    if not _SWINGS_SIM_FIRST_PRINTED:
        _SWINGS_SIM_FIRST_PRINTED = True
        # print("[SWINGS_SIM_START]")
        # print("timeframe:", timeframe)
        # print("ts:", ts)
        # print("swing_highs:", ctx.swing_highs)
        # print("swing_lows:", ctx.swing_lows)
        pass

    # ---------------- Diagnostics: structure_state input ----------------
    try:
        norm_state = _normalize_structure_state(ctx.structure_state)
        _SPOT_EVENT_STRUCTURE_STATE_COUNTS[norm_state or ""] += 1
        _SPOT_EVENT_STRUCTURE_STATE_COUNTS_BY_TF[f"{timeframe}:{norm_state or ''}"] += 1
        global _SPOT_EVENT_LAST_STRUCTURE_STATE
        _SPOT_EVENT_LAST_STRUCTURE_STATE = norm_state
    except Exception:
        pass

    # Debug gating (first N candles per symbol+tf only)
    dbg_key = (symbol, timeframe)
    dbg_seen = _dbg_seen_counts.get(dbg_key, 0)
    dbg_on = bool(DEBUG and ts and (dbg_seen < SPOT_EVENT_FIRST_N))
    if dbg_on:
        _dbg_seen_counts[dbg_key] = dbg_seen + 1

    # Capture what fires on THIS candle (pure logging)
    dbg_disp_et: Optional[str] = None
    dbg_bos_et: Optional[str] = None
    dbg_choch_et: Optional[str] = None
    dbg_sweep_et: Optional[str] = None
    dbg_fvg_events: List[Dict[str, Any]] = []

    # Initialize from prev or defaults
    # Keep existing state behavior (no logic changes here):
    # events_latest starts from previous and is updated below.
    events_latest = copy.deepcopy(ctx.prev_events_latest) if ctx.prev_events_latest else init_events_latest()
    events_active = copy.deepcopy(ctx.prev_events_active) if ctx.prev_events_active else init_events_active()
    events_recent = copy.deepcopy(ctx.prev_events_recent) if ctx.prev_events_recent else init_events_recent()

    before_snapshot = (json.dumps(events_latest, sort_keys=True, default=str),
                       json.dumps(events_active, sort_keys=True, default=str),
                       len(events_recent))

    # -------------------------
    # 1) Displacement
    # -------------------------
    _SPOT_EVENT_DETECTOR_CALL_COUNTS["detect_displacement"] += 1
    disp = detect_displacement(last_candle)
    displacement_fired = False
    if disp and ts:
        et, score, meta = disp
        ev = _make_event(et, ts, timeframe, score, meta)
        events_latest[et] = ev
        _append_recent_unique(events_recent, ev)
        displacement_fired = True
        dbg_disp_et = et

    # -------------------------
    # 2) Liquidity Sweep
    # -------------------------
    sweep = None
    if ts:
        _SPOT_EVENT_DETECTOR_CALL_COUNTS["detect_liquidity_sweep"] += 1
        sweep = detect_liquidity_sweep(last_candle, ctx.liquidity_pool_row, events_recent, timeframe)
    if sweep and ts:
        et, score, meta, level_id = sweep
        ev = _make_event(et, ts, timeframe, score, meta, extra_id=level_id)
        events_latest[et] = ev
        _append_recent_unique(events_recent, ev)
        dbg_sweep_et = et

    # -------------------------
    # 3) BOS trigger
    # -------------------------
    structural_highs = ctx.structural_highs or []
    structural_lows = ctx.structural_lows or []
    bos = None
    if ts:
        _SPOT_EVENT_DETECTOR_CALL_COUNTS["detect_bos"] += 1
        bos = detect_bos(last_candle, structural_highs, structural_lows, displacement_fired)

    if bos and ts:
        et, score, meta = bos
        ev = _make_event(et, ts, timeframe, score, meta)
        events_latest[et] = ev
        _append_recent_unique(events_recent, ev)
        dbg_bos_et = et

        # Start / refresh BOS tracker
        if et == "structure_break_up":
            slot = "bos_bull_tracker"
            opposite_slot = "bos_bear_tracker"

            # Expire opposite BOS active if exists
            if events_active.get(opposite_slot, {}).get("active"):
                events_active[opposite_slot]["expired"] = _expire_tracker(events_active[opposite_slot]["active"], ts, "opposite_bos")
                events_active[opposite_slot]["active"] = None

            ref_level = meta["ref_level"]
            ref_ts = meta["ref_ts"]
            new_t = _new_tracker(
                tracker_type="bos_bull_tracker",
                anchor_event="structure_break_up",
                anchor_ts=ts,
                timeframe=timeframe,
                ref_type="structural",
                ref_level=ref_level,
                ref_ts=ref_ts,
                seed_candle=last_candle,
            )
            events_active[slot] = _slot_set_new_active(events_active.get(slot) or {"active": None, "expired": None}, new_t, ts, "refresh_same_direction")

        if et == "structure_break_down":
            slot = "bos_bear_tracker"
            opposite_slot = "bos_bull_tracker"

            if events_active.get(opposite_slot, {}).get("active"):
                events_active[opposite_slot]["expired"] = _expire_tracker(events_active[opposite_slot]["active"], ts, "opposite_bos")
                events_active[opposite_slot]["active"] = None

            ref_level = meta["ref_level"]
            ref_ts = meta["ref_ts"]
            new_t = _new_tracker(
                tracker_type="bos_bear_tracker",
                anchor_event="structure_break_down",
                anchor_ts=ts,
                timeframe=timeframe,
                ref_type="structural",
                ref_level=ref_level,
                ref_ts=ref_ts,
                seed_candle=last_candle,
            )
            events_active[slot] = _slot_set_new_active(events_active.get(slot) or {"active": None, "expired": None}, new_t, ts, "refresh_same_direction")

    # -------------------------
    # 4) CHOCH trigger
    # -------------------------
    swing_highs = ctx.swing_highs or []
    swing_lows = ctx.swing_lows or []
    choch = None
    if ts:
        _SPOT_EVENT_DETECTOR_CALL_COUNTS["detect_choch"] += 1
        # DEBUG: inject timeframe into last_candle for logging only (no detection logic change)
        lc = last_candle
        try:
            if isinstance(last_candle, dict) and ctx.timeframe:
                if not (last_candle.get("tf") or last_candle.get("timeframe")):
                    lc = dict(last_candle)
                    lc["tf"] = ctx.timeframe
        except Exception:
            lc = last_candle

        choch = detect_choch(lc, swing_highs, swing_lows, ctx.structure_state)

    if choch and ts:
        et, score, meta = choch
        ev = _make_event(et, ts, timeframe, score, meta)
        events_latest[et] = ev
        _append_recent_unique(events_recent, ev)
        dbg_choch_et = et

        # Start / refresh CHOCH tracker
        if et == "choch_up":
            slot = "choch_bull_tracker"
            opposite_slot = "choch_bear_tracker"

            if events_active.get(opposite_slot, {}).get("active"):
                events_active[opposite_slot]["expired"] = _expire_tracker(events_active[opposite_slot]["active"], ts, "opposite_choch")
                events_active[opposite_slot]["active"] = None

            ref_level = meta["ref_level"]
            ref_ts = meta["ref_ts"]
            new_t = _new_tracker(
                tracker_type="choch_bull_tracker",
                anchor_event="choch_up",
                anchor_ts=ts,
                timeframe=timeframe,
                ref_type="swing",
                ref_level=ref_level,
                ref_ts=ref_ts,
                seed_candle=last_candle,
            )
            events_active[slot] = _slot_set_new_active(events_active.get(slot) or {"active": None, "expired": None}, new_t, ts, "refresh_same_direction")

        if et == "choch_down":
            slot = "choch_bear_tracker"
            opposite_slot = "choch_bull_tracker"

            if events_active.get(opposite_slot, {}).get("active"):
                events_active[opposite_slot]["expired"] = _expire_tracker(events_active[opposite_slot]["active"], ts, "opposite_choch")
                events_active[opposite_slot]["active"] = None

            ref_level = meta["ref_level"]
            ref_ts = meta["ref_ts"]
            new_t = _new_tracker(
                tracker_type="choch_bear_tracker",
                anchor_event="choch_down",
                anchor_ts=ts,
                timeframe=timeframe,
                ref_type="swing",
                ref_level=ref_level,
                ref_ts=ref_ts,
                seed_candle=last_candle,
            )
            events_active[slot] = _slot_set_new_active(events_active.get(slot) or {"active": None, "expired": None}, new_t, ts, "refresh_same_direction")

    # -------------------------
    # 5) Update trackers (AVWAP + sub-states)
    # -------------------------
    if ts:
        for slot in TRACKER_SLOTS:
            slot_obj = events_active.get(slot)
            if not isinstance(slot_obj, dict):
                continue
            active = slot_obj.get("active")
            if isinstance(active, dict) and active.get("status") == "active":
                updated = _update_tracker_states(active, last_candle)
                slot_obj["active"] = updated

                # Optional: expire on hard structure rejection (no timeout; only rejection/replacement)
                if updated.get("structure_state") == "rejected":
                    slot_obj["expired"] = _expire_tracker(updated, ts, "structure_rejected")
                    slot_obj["active"] = None

    # -------------------------
    # 6) FVG transition events (created/tapped/filled)
    # -------------------------
    if ts and ctx.fvgs_now is not None:
        _SPOT_EVENT_DETECTOR_CALL_COUNTS["detect_fvg_transitions"] += 1
        fvg_events = detect_fvg_transitions(ctx.fvgs_now, ctx.fvgs_prev, ts, timeframe)
        dbg_fvg_events = fvg_events or []
        for ev in (fvg_events or []):
            et = ev["type"]
            events_latest[et] = ev
            _append_recent_unique(events_recent, ev)

    # ---- Full debug block: first N candles only (pure logging) ----
    if dbg_on:
        mom_atr = last_candle.get("mom_atr")
        vol_rel = last_candle.get("vol_rel")
        spread_strength = last_candle.get("spread_strength")
        cluster_state = ((last_candle.get("cluster") or {}).get("state") or "")

        sh = len(ctx.structural_highs or [])
        sl = len(ctx.structural_lows or [])
        swh = len(ctx.swing_highs or [])
        swl = len(ctx.swing_lows or [])
        st = ctx.structure_state


        # Sample a few FVG events emitted on this candle
        n_sample = max(0, SPOT_EVENT_FVG_SAMPLE)
        for i, ev in enumerate((dbg_fvg_events or [])[:n_sample]):
            meta = ev.get("meta") or {}

    after_snapshot = (json.dumps(events_latest, sort_keys=True, default=str),
                      json.dumps(events_active, sort_keys=True, default=str),
                      len(events_recent))

    changed = before_snapshot != after_snapshot

    out = {
        "events_latest": events_latest,
        "events_active": events_active,
        "events_recent": events_recent,
        "changed": changed,
    }

    _SWINGS_SIM_LAST[timeframe] = {
        "ts": ts,
        "swing_highs": ctx.swing_highs,
        "swing_lows": ctx.swing_lows,
    }

    # ---------------- Diagnostics ----------------
    # Count TRUE triggers on this candle.
    # Increment when either:
    # 1) event transitions "not triggered" -> "triggered", OR
    # 2) event stays triggered but payload changes (new event instance).
    try:
        prev_latest = ctx.prev_events_latest if isinstance(ctx.prev_events_latest, dict) else {}
        for k, v_now in (events_latest or {}).items():
            v_prev = prev_latest.get(k)
            was = _is_triggered_value(v_prev)
            now = _is_triggered_value(v_now)
            if not now:
                continue
            if (not was) or (_trigger_signature(v_prev) != _trigger_signature(v_now)):
                _SPOT_EVENT_TRIGGER_COUNTS[str(k)] += 1
                v_now_dict = v_now if isinstance(v_now, dict) else {}
                meta_now = v_now_dict.get("meta") if isinstance(v_now_dict.get("meta"), dict) else {}
                _SPOT_EVENT_TRIGGER_TS[str(k)].append(
                    {
                        "tf": timeframe,
                        "candle_ts": ts,
                        "ref_candle_ts": _extract_ref_candle_ts(meta_now),
                        "ref_level": _extract_ref_level(meta_now),
                    }
                )
    except Exception:
        pass

    return out
