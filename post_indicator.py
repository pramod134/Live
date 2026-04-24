# post_indicator.py
#
# Derived indicators built AFTER calc1 + calc2.
# This file should not compute raw indicators.
# It only derives higher-level regime/context labels.

from typing import Any, Dict, Optional


def compute_post_indicators(
    base_snapshot: Dict[str, Any],
    advanced_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    advanced_snapshot = advanced_snapshot or {}

    structure_state = compute_structure_state_post(
        base_snapshot=base_snapshot,
        advanced_snapshot=advanced_snapshot,
    )

    return {
        "structure_state": structure_state,
    }


def compute_structure_state_post(
    base_snapshot: Dict[str, Any],
    advanced_snapshot: Dict[str, Any],
) -> Optional[str]:
    raw_state = base_snapshot.get("structure_state")
    if not raw_state:
        return None

    state = str(raw_state)

    momentum = advanced_snapshot.get("momentum") or {}
    htf = advanced_snapshot.get("htf") or {}
    vwap = advanced_snapshot.get("vwap") or {}

    mom_strength = momentum.get("mom_strength")
    mom_exhaustion = bool(momentum.get("mom_exhaustion"))

    htf_bias = htf.get("bias")
    vwap_ext = vwap.get("extended_flag")

    # Momentum quality context
    if "continuation_bos" in state:
        if mom_strength == "weak":
            state += "_weak_momentum"
        elif mom_strength in {"strong", "extreme"}:
            state += "_strong_momentum"

    # Exhaustion context
    if mom_exhaustion:
        state += "_exhaustion"

    # HTF alignment context
    if htf_bias in {"bull", "bear"}:
        if "bullish" in state and htf_bias == "bull":
            state += "_htf_aligned"
        elif "bearish" in state and htf_bias == "bear":
            state += "_htf_aligned"
        elif "bullish" in state or "bearish" in state:
            state += "_htf_conflict"

    # VWAP extension context
    if vwap_ext == "extended_above":
        state += "_extended_above_vwap"
    elif vwap_ext == "extended_below":
        state += "_extended_below_vwap"

    return state
