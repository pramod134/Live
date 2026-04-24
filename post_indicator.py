# post_indicator.py
#
# Derived indicators built AFTER calc1 + calc2.
# This file should not compute raw indicators.
# It only derives higher-level regime/context labels.

from typing import Any, Dict, Optional


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def compute_post_indicators(
    base_snapshot: Dict[str, Any],
    advanced_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    advanced_snapshot = advanced_snapshot or {}

    structure_state = compute_structure_state_post(
        base_snapshot=base_snapshot,
        advanced_snapshot=advanced_snapshot,
    )

    return structure_state


def compute_structure_state_post(
    base_snapshot: Dict[str, Any],
    advanced_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    raw_state = base_snapshot.get("structure_state")
    if not raw_state:
        return {
            "structure_state": None,
            "structure_meta": {
                "raw_state": None,
                "action": "AVOID",
                "confidence": 0,
                "risk_tier": "D",
                "tags": ["missing_raw_structure_state"],
            },
        }

    state = str(raw_state)
    state_l = state.lower()

    momentum = advanced_snapshot.get("momentum") or {}
    htf = advanced_snapshot.get("htf") or {}
    vwap = advanced_snapshot.get("vwap") or {}
    vol_context = advanced_snapshot.get("vol_context") or {}

    mom_regime = momentum.get("mom_regime")
    mom_strength = momentum.get("mom_strength")
    mom_exhaustion = bool(momentum.get("mom_exhaustion"))

    htf_bias = htf.get("bias")
    vwap_ext = vwap.get("extended_flag")
    bar_range_vs_atr = _safe_float(vol_context.get("bar_range_vs_atr"))

    last_candle = base_snapshot.get("last_candle") or {}
    vol_rel = _safe_float(last_candle.get("vol_rel"))
    candle_open = _safe_float(last_candle.get("open"))
    candle_close = _safe_float(last_candle.get("close"))

    candle_dir = None
    if candle_open is not None and candle_close is not None:
        if candle_close > candle_open:
            candle_dir = "bullish"
        elif candle_close < candle_open:
            candle_dir = "bearish"
        else:
            candle_dir = "neutral"

    direction = "neutral"
    if "bullish" in state_l:
        direction = "bullish"
    elif "bearish" in state_l:
        direction = "bearish"

    confidence = 50
    tags = []

    # Momentum quality context
    if "continuation_bos" in state:
        if mom_strength == "weak":
            state += "_weak_momentum"
            confidence -= 15
            tags.append("weak_momentum")
        elif mom_strength in {"strong", "extreme"}:
            state += "_strong_momentum"
            confidence += 20
            tags.append("strong_momentum")

    # Exhaustion context
    if mom_exhaustion:
        state += "_exhaustion"
        confidence -= 35
        tags.append("momentum_exhaustion")

    # HTF alignment context
    htf_alignment = "unknown"
    if htf_bias in {"bull", "bear"}:
        if "bullish" in state and htf_bias == "bull":
            state += "_htf_aligned"
            htf_alignment = "aligned"
            confidence += 20
            tags.append("htf_aligned")
        elif "bearish" in state and htf_bias == "bear":
            state += "_htf_aligned"
            htf_alignment = "aligned"
            confidence += 20
            tags.append("htf_aligned")
        elif "bullish" in state or "bearish" in state:
            state += "_htf_conflict"
            htf_alignment = "conflict"
            confidence -= 25
            tags.append("htf_conflict")

    # VWAP extension context
    if vwap_ext == "extended_above":
        state += "_extended_above_vwap"
        confidence -= 20
        tags.append("extended_above_vwap")
    elif vwap_ext == "extended_below":
        state += "_extended_below_vwap"
        confidence -= 20
        tags.append("extended_below_vwap")

    # Current-candle volume confirmation.
    # Uses current candle direction/range, not windowed momentum direction.
    if vol_rel is not None and vol_rel > 1.5:
        if bar_range_vs_atr is not None and bar_range_vs_atr >= 0.6:
            if direction == "bullish" and candle_dir == "bullish":
                confidence += 15
                tags.append("volume_confirmed_bull")
            elif direction == "bearish" and candle_dir == "bearish":
                confidence += 15
                tags.append("volume_confirmed_bear")
        else:
            confidence -= 10
            tags.append("volume_absorption")

    confidence = max(0, min(100, int(round(confidence))))

    if mom_exhaustion or vwap_ext in {"extended_above", "extended_below"}:
        action = "WAIT"
    elif confidence >= 70:
        action = "TRADE"
    elif confidence >= 45:
        action = "WATCH"
    else:
        action = "AVOID"

    if confidence >= 75:
        risk_tier = "A"
    elif confidence >= 55:
        risk_tier = "B"
    elif confidence >= 35:
        risk_tier = "C"
    else:
        risk_tier = "D"

    return {
        "structure_state": state,
        "structure_meta": {
            "raw_state": raw_state,
            "direction": direction,
            "momentum_regime": mom_regime,
            "momentum_strength": mom_strength,
            "momentum_exhaustion": mom_exhaustion,
            "htf_alignment": htf_alignment,
            "vwap_context": vwap_ext,
            "volume_rel": vol_rel,
            "candle_dir": candle_dir,
            "bar_range_vs_atr": bar_range_vs_atr,
            "action": action,
            "confidence": confidence,
            "risk_tier": risk_tier,
            "tags": tags,
        },
    }
