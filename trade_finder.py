# Input: skinny snapshot dict
# Output: raw trade ideas list
# No DB writes. No execution. No dedupe. No mutation.
# Applies final same-batch output cleanup after raw ideas are generated.

import datetime as dt
from typing import Any, Dict, List, Optional


HORIZONS = {
    "swing": ["1d", "1h", "15m"],
    "day": ["1h", "15m", "5m"],
    "scalp": ["5m", "3m", "1m"],
}

# --- V3 constants ---
BREAKOUT_EXPANSION_THRESHOLD = 1.5
VWAP_PULLBACK_ATR_FACTOR = 0.25
BATCH_LEVEL_TOLERANCE = 0.10
MIN_TP_ROOM = 0.50
MIN_CONFIDENCE_FOR_TIGHT_TP = 50

# --- Quality scoring ---
VP_CONFLUENCE_WEIGHTS = {
    "structural": 8,
    "rolling_60": 5,
    "session": 3,
}
VP_CONFLUENCE_MAX_BONUS = 10


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _get_market(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    return snapshot.get("market") or snapshot


def _tf(market: Dict[str, Any], tf: str) -> Dict[str, Any]:
    return (market.get("tfs") or {}).get(tf) or {}


def _tf_delta(tf: str) -> Optional[dt.timedelta]:
    tf = str(tf or "").strip().lower()
    if tf.endswith("m"):
        return dt.timedelta(minutes=int(tf[:-1]))
    if tf.endswith("h"):
        return dt.timedelta(hours=int(tf[:-1]))
    if tf.endswith("d"):
        return dt.timedelta(days=int(tf[:-1]))
    return None


def _candle_close_timestamp(open_ts: Any, tf: str) -> Optional[str]:
    if open_ts is None:
        return None

    delta = _tf_delta(tf)
    if delta is None:
        return str(open_ts)

    try:
        if isinstance(open_ts, dt.datetime):
            ts = open_ts
        else:
            ts = dt.datetime.fromisoformat(str(open_ts).replace("Z", "+00:00"))

        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)

        return (ts + delta).isoformat()
    except Exception:
        return str(open_ts)


def _level_price(obj: Any) -> Optional[float]:
    if not isinstance(obj, dict):
        return None
    return _safe_float(obj.get("price"))


def _is_stale_fvg(fvg: Dict[str, Any]) -> bool:
    freshness = (fvg.get("freshness") or {}).get("freshness")
    return freshness == "stale" or freshness == "invalid_future"


def _is_filled_fvg(fvg: Dict[str, Any]) -> bool:
    return bool(fvg.get("filled_ts"))


def _all_fvgs(fvgs: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for bucket in ("bullish_below", "bearish_above"):
        for f in fvgs.get(bucket) or []:
            if isinstance(f, dict):
                out.append(f)
    return out


def _add_target(targets: List[Dict[str, Any]], price: Any, source: str, direction: str) -> None:
    p = _safe_float(price)
    if p is None:
        return

    for t in targets:
        if abs(float(t["price"]) - p) < 0.01:
            return

    targets.append(
        {
            "price": round(p, 4),
            "source": source,
            "direction": direction,
        }
    )


def _filter_invalid_targets(
    *,
    targets: List[Dict[str, Any]],
    entry_low: float,
    entry_high: float,
    tf_data: Dict[str, Any],
    direction: str,
) -> List[Dict[str, Any]]:
    floor = _safe_float((tf_data.get("atr_buffers") or {}).get("normal")) or 0.0
    valid: List[Dict[str, Any]] = []

    for t in targets:
        p = _safe_float(t.get("price"))
        if p is None:
            continue

        if direction == "long":
            if p <= entry_low:
                continue
            if floor > 0 and (p - entry_low) < floor:
                continue

        elif direction == "short":
            if p >= entry_high:
                continue
            if floor > 0 and (entry_high - p) < floor:
                continue

        valid.append(t)

    return valid


def _tp1_price(trade: Dict[str, Any]) -> Optional[float]:
    targets = trade.get("targets") or []
    if not targets or not isinstance(targets[0], dict):
        return None
    return _safe_float(targets[0].get("price"))


def _trade_geometry(trade: Dict[str, Any]) -> Optional[Dict[str, float]]:
    entry = trade.get("entry_zone") or {}
    stop = trade.get("stop") or {}

    entry_low = _safe_float(entry.get("low"))
    entry_high = _safe_float(entry.get("high"))
    stop_price = _safe_float(stop.get("price"))
    tp1 = _tp1_price(trade)

    if entry_low is None or entry_high is None or stop_price is None or tp1 is None:
        return None

    return {
        "entry_low": float(entry_low),
        "entry_high": float(entry_high),
        "stop": float(stop_price),
        "tp1": float(tp1),
    }


def _same_batch_merge_scope(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    return (
        a.get("symbol") == b.get("symbol")
        and a.get("tf") == b.get("tf")
        and a.get("direction") == b.get("direction")
    )


def _geometry_within_tolerance(
    a: Dict[str, Any],
    b: Dict[str, Any],
    *,
    tolerance: float = BATCH_LEVEL_TOLERANCE,
) -> bool:
    ga = _trade_geometry(a)
    gb = _trade_geometry(b)
    if ga is None or gb is None:
        return False

    return (
        abs(ga["entry_low"] - gb["entry_low"]) <= tolerance
        and abs(ga["entry_high"] - gb["entry_high"]) <= tolerance
        and abs(ga["stop"] - gb["stop"]) <= tolerance
        and abs(ga["tp1"] - gb["tp1"]) <= tolerance
    )


def _merge_same_batch_trades(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """
    Same-batch fuzzy merge.
    Keep latest trade metadata, broaden entry zone, append strategy names into
    the existing strategy field, and combine lightweight metadata.
    """
    merged = dict(new)

    old_entry = old.get("entry_zone") or {}
    new_entry = new.get("entry_zone") or {}
    old_low = _safe_float(old_entry.get("low"))
    old_high = _safe_float(old_entry.get("high"))
    new_low = _safe_float(new_entry.get("low"))
    new_high = _safe_float(new_entry.get("high"))

    entry = dict(new_entry)
    if old_low is not None and new_low is not None:
        entry["low"] = round(min(float(old_low), float(new_low)), 4)
    if old_high is not None and new_high is not None:
        entry["high"] = round(max(float(old_high), float(new_high)), 4)
    merged["entry_zone"] = entry

    merged["merge_count"] = int(old.get("merge_count") or 1) + int(new.get("merge_count") or 1)

    old_horizons = old.get("horizons") or [old.get("horizon")]
    new_horizons = new.get("horizons") or [new.get("horizon")]
    merged["horizons"] = sorted({h for h in old_horizons + new_horizons if h})

    old_strategy_value = old.get("strategy")
    new_strategy_value = new.get("strategy")
    old_strategies = str(old_strategy_value).split("+") if old_strategy_value else []
    new_strategies = str(new_strategy_value).split("+") if new_strategy_value else []
    strategy_list = sorted({s for s in old_strategies + new_strategies if s})
    merged["strategy"] = "+".join(strategy_list)

    merged["tags"] = sorted(set((old.get("tags") or []) + (new.get("tags") or [])))
    merged["warnings"] = sorted(set((old.get("warnings") or []) + (new.get("warnings") or [])))

    return merged


def _same_batch_fuzzy_merge(ideas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []

    for idea in ideas:
        matched_idx = None
        for idx, existing in enumerate(merged):
            if not _same_batch_merge_scope(existing, idea):
                continue
            if _geometry_within_tolerance(existing, idea):
                matched_idx = idx
                break

        if matched_idx is None:
            merged.append(idea)
        else:
            merged[matched_idx] = _merge_same_batch_trades(merged[matched_idx], idea)

    return merged


def _passes_tp_room_confidence_filter(trade: Dict[str, Any]) -> bool:
    entry = trade.get("entry_zone") or {}
    direction = trade.get("direction")
    tp1 = _tp1_price(trade)
    confidence = _safe_float(trade.get("confidence")) or 0.0

    if tp1 is None:
        return False

    if direction == "long":
        entry_low = _safe_float(entry.get("low"))
        if entry_low is None:
            return False
        room = float(tp1) - float(entry_low)

    elif direction == "short":
        entry_high = _safe_float(entry.get("high"))
        if entry_high is None:
            return False
        room = float(entry_high) - float(tp1)

    else:
        return False

    return not (room < MIN_TP_ROOM and confidence < MIN_CONFIDENCE_FOR_TIGHT_TP)


def _filter_low_quality_tight_tp_ideas(ideas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [idea for idea in ideas if _passes_tp_room_confidence_filter(idea)]


def _range_overlaps(a_low: float, a_high: float, b_low: Any, b_high: Any) -> bool:
    bl = _safe_float(b_low)
    bh = _safe_float(b_high)
    if bl is None or bh is None:
        return False
    lo = min(bl, bh)
    hi = max(bl, bh)
    return max(float(a_low), lo) <= min(float(a_high), hi)


def _price_in_entry(price: Any, entry_low: float, entry_high: float, buffer: float = 0.0) -> bool:
    p = _safe_float(price)
    if p is None:
        return False
    return (float(entry_low) - buffer) <= p <= (float(entry_high) + buffer)


def _vp_node_overlaps_entry(node: Any, entry_low: float, entry_high: float, buffer: float) -> bool:
    if not isinstance(node, dict):
        return False
    if _range_overlaps(entry_low - buffer, entry_high + buffer, node.get("low"), node.get("high")):
        return True
    return _price_in_entry((node.get("poc") or {}).get("price"), entry_low, entry_high, buffer)


def _build_targets(
    *,
    market: Dict[str, Any],
    tf_data: Dict[str, Any],
    direction: str,
    horizon: str,
) -> Dict[str, Any]:
    targets: List[Dict[str, Any]] = []

    liquidity = tf_data.get("liquidity") or {}
    structural = tf_data.get("structural_levels") or {}
    vp = tf_data.get("volume_profile") or {}
    macro = (market.get("macro") or {}).get("weekly") or {}

    if direction == "long":
        _add_target(targets, _level_price(liquidity.get("nearest_clean_high")), "nearest_clean_high", direction)
        _add_target(targets, _level_price(structural.get("nearest_high_above")), "nearest_high_above", direction)

        for profile_name in ("session", "structural", "rolling_60"):
            profile = vp.get(profile_name) or {}
            _add_target(
                targets,
                _level_price((profile.get("hvn_above") or {}).get("poc")),
                f"{profile_name}_hvn_above",
                direction,
            )
            _add_target(
                targets,
                _level_price((profile.get("lvn_above") or {}).get("poc")),
                f"{profile_name}_lvn_above",
                direction,
            )

        if horizon == "swing":
            _add_target(targets, _level_price(macro.get("nearest_clean_high")), "weekly_clean_high", direction)
            _add_target(targets, _level_price(macro.get("nearest_swing_high")), "weekly_swing_high", direction)

    else:
        _add_target(targets, _level_price(liquidity.get("nearest_clean_low")), "nearest_clean_low", direction)
        _add_target(targets, _level_price(structural.get("nearest_low_below")), "nearest_low_below", direction)

        for profile_name in ("session", "structural", "rolling_60"):
            profile = vp.get(profile_name) or {}
            _add_target(
                targets,
                _level_price((profile.get("hvn_below") or {}).get("poc")),
                f"{profile_name}_hvn_below",
                direction,
            )
            _add_target(
                targets,
                _level_price((profile.get("lvn_below") or {}).get("poc")),
                f"{profile_name}_lvn_below",
                direction,
            )

        if horizon == "swing":
            _add_target(targets, _level_price(macro.get("nearest_clean_low")), "weekly_clean_low", direction)
            _add_target(targets, _level_price(macro.get("nearest_swing_low")), "weekly_swing_low", direction)

    count_before_limit = len(targets)
    limit = 2 if horizon == "scalp" else 3 if horizon == "day" else 4

    return {
        "targets": targets[:limit],
        "target_count_before_limit": count_before_limit,
        "targets_truncated": count_before_limit > limit,
    }


def _score_trade(
    *,
    tf_data: Dict[str, Any],
    direction: str,
    trade_type: str,
    entry_low: float,
    entry_high: float,
    stop_price: float,
    targets: List[Dict[str, Any]],
    market_bias: Optional[str],
) -> Dict[str, Any]:
    structure = tf_data.get("structure") or {}
    momentum = tf_data.get("momentum") or {}
    vwap = tf_data.get("vwap") or {}

    confidence = structure.get("confidence")
    score = int(confidence if confidence is not None else 50)

    tags = list(structure.get("tags") or [])
    warnings: List[str] = []
    reasons: List[str] = []

    mom_strength = momentum.get("mom_strength")
    macd_hist = _safe_float(momentum.get("macd_hist"))
    macd_slope = _safe_float(momentum.get("macd_hist_slope_3"))

    if mom_strength in {"strong", "extreme"}:
        score += 8
        tags.append("momentum_strong")
        reasons.append("momentum is strong")
    elif mom_strength == "weak":
        score -= 10
        warnings.append("weak_momentum")

    if direction == "long":
        if macd_hist is not None and macd_hist > 0:
            score += 5
            tags.append("macd_confirmed")
            reasons.append("MACD histogram confirms long bias")
        elif macd_hist is not None:
            score -= 5
            warnings.append("macd_not_confirmed")

        if macd_slope is not None and macd_slope >= 0:
            score += 5
            tags.append("macd_improving")
            reasons.append("MACD histogram slope is improving")
        elif macd_slope is not None:
            score -= 5
            warnings.append("macd_fading")

    elif direction == "short":
        if macd_hist is not None and macd_hist < 0:
            score += 5
            tags.append("macd_confirmed")
            reasons.append("MACD histogram confirms short bias")
        elif macd_hist is not None:
            score -= 5
            warnings.append("macd_not_confirmed")

        if macd_slope is not None and macd_slope <= 0:
            score += 5
            tags.append("macd_improving")
            reasons.append("MACD histogram slope supports downside")
        elif macd_slope is not None:
            score -= 5
            warnings.append("macd_fading")

    if market_bias in {"bullish", "bearish"}:
        if direction == "long" and market_bias == "bullish":
            score += 5
            tags.append("mtf_aligned")
            reasons.append("trade direction aligns with MTF bias")
        elif direction == "short" and market_bias == "bearish":
            score += 5
            tags.append("mtf_aligned")
            reasons.append("trade direction aligns with MTF bias")
        else:
            score -= 8
            warnings.append("against_mtf_bias")

    if vwap.get("extended_flag") in {"extended_above", "extended_below"}:
        score -= 8
        warnings.append("vwap_extended")

    if trade_type == "reversal":
        score -= 5
        tags.append("reversal")
    else:
        tags.append("continuation")

    rr = None
    if targets:
        entry_mid = (entry_low + entry_high) / 2.0
        risk = abs(entry_mid - stop_price)
        reward = abs(float(targets[0]["price"]) - entry_mid)
        if risk > 0:
            rr = reward / risk
            if rr >= 2:
                score += 8
                tags.append("rr_good")
                reasons.append("RR to TP1 is strong")
            elif rr < 1:
                score -= 15
                warnings.append("rr_poor")

    vp = tf_data.get("volume_profile") or {}
    atr_tight = _safe_float((tf_data.get("atr_buffers") or {}).get("tight")) or 0.0
    confluence_bonus = 0
    confluence_profiles: List[str] = []

    for profile_name, weight in VP_CONFLUENCE_WEIGHTS.items():
        profile = vp.get(profile_name) or {}
        matched = False

        if _vp_node_overlaps_entry(profile.get("hvn_here"), entry_low, entry_high, atr_tight):
            matched = True

        if not matched:
            poc_price = (profile.get("poc") or {}).get("price")
            if _price_in_entry(poc_price, entry_low, entry_high, atr_tight):
                matched = True

        if matched:
            confluence_bonus += weight
            confluence_profiles.append(profile_name)

    if confluence_bonus > 0:
        applied_bonus = min(VP_CONFLUENCE_MAX_BONUS, confluence_bonus)
        score += applied_bonus
        tags.append("vp_confluence")
        tags.append("hvn_confluence")

        if "structural" in confluence_profiles:
            tags.append("structural_hvn_confluence")
        if "rolling_60" in confluence_profiles:
            tags.append("rolling_60_hvn_confluence")
        if "session" in confluence_profiles:
            tags.append("session_hvn_confluence")

        reasons.append(
            "entry overlaps volume-profile confluence: "
            + ",".join(sorted(set(confluence_profiles)))
        )

    return {
        "score": max(0, min(100, int(round(score)))),
        "tags": sorted(set(tags)),
        "warnings": sorted(set(warnings)),
        "rr_to_tp1": round(rr, 3) if rr is not None else None,
        "reasons": reasons,
    }


def _make_trade(
    *,
    market: Dict[str, Any],
    tf: str,
    tf_data: Dict[str, Any],
    horizon: str,
    trade_type: str,
    strategy: str,
    direction: str,
    entry_low: float,
    entry_high: float,
    stop_price: float,
    entry_source: str,
    stop_source: str,
    tf_timestamp: Optional[str] = None,
    extra_tags: Optional[List[str]] = None,
    extra_reasons: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    if tf_timestamp is None:
        tf_timestamp = _candle_close_timestamp(
            tf_data.get("asof") or tf_data.get("last_ts"),
            tf,
        )

    target_obj = _build_targets(
        market=market,
        tf_data=tf_data,
        direction=direction,
        horizon=horizon,
    )

    targets = target_obj["targets"]
    if not targets:
        return None

    targets = _filter_invalid_targets(
        targets=targets,
        entry_low=entry_low,
        entry_high=entry_high,
        tf_data=tf_data,
        direction=direction,
    )
    if not targets:
        return None

    market_bias = (market.get("summary") or {}).get("bias")
    scoring = _score_trade(
        tf_data=tf_data,
        direction=direction,
        trade_type=trade_type,
        entry_low=entry_low,
        entry_high=entry_high,
        stop_price=stop_price,
        targets=targets,
        market_bias=market_bias,
    )

    tags = list(scoring["tags"])
    warnings = list(scoring["warnings"])
    reasons = list(scoring["reasons"])

    if extra_tags:
        tags.extend(extra_tags)
    if extra_reasons:
        reasons.extend(extra_reasons)

    if target_obj.get("targets_truncated"):
        warnings.append("targets_truncated")

    structure = tf_data.get("structure") or {}

    reason = "; ".join(reasons) if reasons else f"{strategy} trade idea from {tf}"

    return {
        "symbol": market.get("symbol"),
        "horizon": horizon,
        "tf": tf,
        "tf_timestamp": tf_timestamp,
        "trade_type": trade_type,
        "strategy": strategy,
        "direction": direction,
        "entry_zone": {
            "low": round(float(entry_low), 4),
            "high": round(float(entry_high), 4),
            "source": entry_source,
        },
        "stop": {
            "price": round(float(stop_price), 4),
            "source": stop_source,
        },
        "targets": targets,
        "target_count_before_limit": target_obj.get("target_count_before_limit"),
        "score": scoring["score"],
        "confidence": structure.get("confidence"),
        "risk_tier": structure.get("risk_tier"),
        "rr_to_tp1": scoring["rr_to_tp1"],
        "tags": sorted(set(tags)),
        "warnings": sorted(set(warnings)),
        "reason": reason,
        "source_tfs": HORIZONS.get(horizon, [tf]),
        "raw_structure_state": structure.get("raw"),
        "structure_state": structure.get("state"),
    }


def _continuation_ideas(market: Dict[str, Any], horizon: str, tf: str, tf_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    ideas: List[Dict[str, Any]] = []

    structure = tf_data.get("structure") or {}
    state = str(structure.get("state") or "").lower()
    direction = structure.get("direction")
    fvgs = tf_data.get("fvgs") or {}

    # allow multi-strategy continuation (no early return)
    momentum = tf_data.get("momentum") or {}
    vwap = tf_data.get("vwap") or {}
    vol_ctx = tf_data.get("vol_context") or {}

    if "bos" in state or "choch" in state:
        strategy = "choch_fvg_pullback" if "choch" in state else "bos_fvg_pullback"
        all_fvgs = [f for f in _all_fvgs(fvgs) if not _is_filled_fvg(f)]

        if direction == "bullish":
            long_fvgs = [f for f in all_fvgs if f.get("direction") == "bull"]

            for fvg in long_fvgs:
                low = _level_price(fvg.get("low"))
                high = _level_price(fvg.get("high"))
                stop = _safe_float((fvg.get("low") or {}).get("long_stop_ref"))
                if low is None or high is None or stop is None:
                    continue

                trade = _make_trade(
                    market=market,
                    tf=tf,
                    tf_data=tf_data,
                    horizon=horizon,
                    trade_type="continuation",
                    strategy=strategy,
                    direction="long",
                    entry_low=low,
                    entry_high=high,
                    stop_price=stop,
                    entry_source=f"{tf}_bullish_fvg",
                    stop_source="fvg_low_normal_atr_buffer",
                    extra_reasons=[f"{tf} bullish structure with bullish FVG pullback"],
                )
                if trade:
                    ideas.append(trade)

        elif direction == "bearish":
            short_fvgs = [f for f in all_fvgs if f.get("direction") == "bear"]

            for fvg in short_fvgs:
                low = _level_price(fvg.get("low"))
                high = _level_price(fvg.get("high"))
                stop = _safe_float((fvg.get("high") or {}).get("short_stop_ref"))
                if low is None or high is None or stop is None:
                    continue

                trade = _make_trade(
                    market=market,
                    tf=tf,
                    tf_data=tf_data,
                    horizon=horizon,
                    trade_type="continuation",
                    strategy=strategy,
                    direction="short",
                    entry_low=low,
                    entry_high=high,
                    stop_price=stop,
                    entry_source=f"{tf}_bearish_fvg",
                    stop_source="fvg_high_normal_atr_buffer",
                    extra_reasons=[f"{tf} bearish structure with bearish FVG pullback"],
                )
                if trade:
                    ideas.append(trade)

    # --- VWAP CONTINUATION ---
    vwap_price = _safe_float(vwap.get("vwap"))
    side = vwap.get("side")
    atr = _safe_float(momentum.get("atr")) or 0.0

    if vwap_price is not None and atr > 0:
        band = atr * VWAP_PULLBACK_ATR_FACTOR

        if direction == "bullish" and side == "above":
            trade = _make_trade(
                market=market,
                tf=tf,
                tf_data=tf_data,
                horizon=horizon,
                trade_type="continuation",
                strategy="vwap_continuation",
                direction="long",
                entry_low=vwap_price - band,
                entry_high=vwap_price + band,
                stop_price=vwap_price - atr,
                entry_source=f"{tf}_vwap_pullback",
                stop_source="vwap_minus_atr",
                extra_tags=["vwap_hold"],
                extra_reasons=[f"{tf} bullish structure holding above VWAP"],
            )
            if trade:
                ideas.append(trade)

        if direction == "bearish" and side == "below":
            trade = _make_trade(
                market=market,
                tf=tf,
                tf_data=tf_data,
                horizon=horizon,
                trade_type="continuation",
                strategy="vwap_continuation",
                direction="short",
                entry_low=vwap_price - band,
                entry_high=vwap_price + band,
                stop_price=vwap_price + atr,
                entry_source=f"{tf}_vwap_pullback",
                stop_source="vwap_plus_atr",
                extra_tags=["vwap_hold"],
                extra_reasons=[f"{tf} bearish structure holding below VWAP"],
            )
            if trade:
                ideas.append(trade)

    # --- BREAKOUT CONTINUATION ---
    expansion = _safe_float(vol_ctx.get("bar_range_vs_atr"))
    atr = _safe_float(momentum.get("atr")) or 0.0

    if expansion is not None and expansion > BREAKOUT_EXPANSION_THRESHOLD and atr > 0:
        structural = tf_data.get("structural_levels") or {}

        if direction == "bullish":
            level = _level_price(structural.get("nearest_high_above"))
            if level:
                trade = _make_trade(
                    market=market,
                    tf=tf,
                    tf_data=tf_data,
                    horizon=horizon,
                    trade_type="continuation",
                    strategy="breakout_continuation",
                    direction="long",
                    entry_low=level,
                    entry_high=level + atr,
                    stop_price=level - atr,
                    entry_source=f"{tf}_breakout",
                    stop_source="breakout_minus_atr",
                    extra_tags=["breakout", "expansion"],
                    extra_reasons=[f"{tf} bullish breakout with expansion"],
                )
                if trade:
                    ideas.append(trade)

        if direction == "bearish":
            level = _level_price(structural.get("nearest_low_below"))
            if level:
                trade = _make_trade(
                    market=market,
                    tf=tf,
                    tf_data=tf_data,
                    horizon=horizon,
                    trade_type="continuation",
                    strategy="breakout_continuation",
                    direction="short",
                    entry_low=level - atr,
                    entry_high=level,
                    stop_price=level + atr,
                    entry_source=f"{tf}_breakdown",
                    stop_source="breakout_plus_atr",
                    extra_tags=["breakout", "expansion"],
                    extra_reasons=[f"{tf} bearish breakdown with expansion"],
                )
                if trade:
                    ideas.append(trade)

    return ideas


def _reversal_ideas(market: Dict[str, Any], horizon: str, tf: str, tf_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    ideas: List[Dict[str, Any]] = []

    liquidity = tf_data.get("liquidity") or {}
    momentum = tf_data.get("momentum") or {}
    atr_buffers = tf_data.get("atr_buffers") or {}
    structure = tf_data.get("structure") or {}

    normal_atr = _safe_float(atr_buffers.get("normal")) or _safe_float(momentum.get("atr")) or 0.0
    macd_slope = _safe_float(momentum.get("macd_hist_slope_3"))

    swept_below = liquidity.get("nearest_swept_below")
    swept_above = liquidity.get("nearest_swept_above")

    if isinstance(swept_below, dict):
        level = _level_price(swept_below)
        if level is not None and (macd_slope is None or macd_slope >= 0):
            trade = _make_trade(
                market=market,
                tf=tf,
                tf_data=tf_data,
                horizon=horizon,
                trade_type="reversal",
                strategy="liquidity_sweep_reversal",
                direction="long",
                entry_low=level,
                entry_high=level + normal_atr,
                stop_price=level - normal_atr,
                entry_source=f"{tf}_nearest_swept_below_reclaim",
                stop_source="sweep_low_minus_normal_atr",
                extra_tags=["liquidity_sweep"],
                extra_reasons=[f"{tf} swept liquidity below and MACD slope is not bearish"],
            )
            if trade:
                ideas.append(trade)

    if isinstance(swept_above, dict):
        level = _level_price(swept_above)
        if level is not None and (macd_slope is None or macd_slope <= 0):
            trade = _make_trade(
                market=market,
                tf=tf,
                tf_data=tf_data,
                horizon=horizon,
                trade_type="reversal",
                strategy="liquidity_sweep_reversal",
                direction="short",
                entry_low=level - normal_atr,
                entry_high=level,
                stop_price=level + normal_atr,
                entry_source=f"{tf}_nearest_swept_above_reclaim",
                stop_source="sweep_high_plus_normal_atr",
                extra_tags=["liquidity_sweep"],
                extra_reasons=[f"{tf} swept liquidity above and MACD slope is not bullish"],
            )
            if trade:
                ideas.append(trade)

    # --- VWAP RECLAIM REVERSAL ---
    vwap = tf_data.get("vwap") or {}
    vwap_price = _safe_float(vwap.get("vwap"))
    side = vwap.get("side")
    atr = _safe_float(momentum.get("atr")) or 0.0

    if vwap_price is not None and atr > 0:
        if side == "below" and structure.get("direction") == "bullish":
            trade = _make_trade(
                market=market,
                tf=tf,
                tf_data=tf_data,
                horizon=horizon,
                trade_type="reversal",
                strategy="vwap_reclaim_reversal",
                direction="long",
                entry_low=vwap_price - (atr * 0.25),
                entry_high=vwap_price + (atr * 0.25),
                stop_price=vwap_price - atr,
                entry_source=f"{tf}_vwap_reclaim",
                stop_source="vwap_minus_atr",
                extra_tags=["vwap_reclaim"],
                extra_reasons=[f"{tf} reclaiming VWAP with bullish structure"],
            )
            if trade:
                ideas.append(trade)

        if side == "above" and structure.get("direction") == "bearish":
            trade = _make_trade(
                market=market,
                tf=tf,
                tf_data=tf_data,
                horizon=horizon,
                trade_type="reversal",
                strategy="vwap_reclaim_reversal",
                direction="short",
                entry_low=vwap_price - (atr * 0.25),
                entry_high=vwap_price + (atr * 0.25),
                stop_price=vwap_price + atr,
                entry_source=f"{tf}_vwap_reclaim",
                stop_source="vwap_plus_atr",
                extra_tags=["vwap_reclaim"],
                extra_reasons=[f"{tf} losing VWAP with bearish structure"],
            )
            if trade:
                ideas.append(trade)

    # --- MOMENTUM EXHAUSTION REVERSAL ---
    if momentum.get("mom_exhaustion"):
        structural = tf_data.get("structural_levels") or {}
        atr = _safe_float(momentum.get("atr")) or 0.0

        if atr > 0:
            if structure.get("direction") == "bullish":
                level = _level_price(structural.get("nearest_high_above"))
                if level is not None:
                    trade = _make_trade(
                        market=market,
                        tf=tf,
                        tf_data=tf_data,
                        horizon=horizon,
                        trade_type="reversal",
                        strategy="exhaustion_reversal",
                        direction="short",
                        entry_low=level - (atr * 0.25),
                        entry_high=level,
                        stop_price=level + atr,
                        entry_source=f"{tf}_exhaustion",
                        stop_source="structure_high_plus_atr",
                        extra_tags=["exhaustion"],
                        extra_reasons=[f"{tf} bullish exhaustion detected"],
                    )
                    if trade:
                        ideas.append(trade)

            if structure.get("direction") == "bearish":
                level = _level_price(structural.get("nearest_low_below"))
                if level is not None:
                    trade = _make_trade(
                        market=market,
                        tf=tf,
                        tf_data=tf_data,
                        horizon=horizon,
                        trade_type="reversal",
                        strategy="exhaustion_reversal",
                        direction="long",
                        entry_low=level,
                        entry_high=level + (atr * 0.25),
                        stop_price=level - atr,
                        entry_source=f"{tf}_exhaustion",
                        stop_source="structure_low_minus_atr",
                        extra_tags=["exhaustion"],
                        extra_reasons=[f"{tf} bearish exhaustion detected"],
                    )
                    if trade:
                        ideas.append(trade)

    return ideas


def find_trade_ideas(skinny_snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    market = _get_market(skinny_snapshot)
    ideas: List[Dict[str, Any]] = []

    for horizon, tfs in HORIZONS.items():
        for tf in tfs:
            tf_data = _tf(market, tf)
            if not tf_data:
                continue
            # TEMP: allow stale TFs during sim/testing
            # if tf_data.get("is_stale"):
            #     continue

            ideas.extend(_continuation_ideas(market, horizon, tf, tf_data))
            ideas.extend(_reversal_ideas(market, horizon, tf, tf_data))

    ideas = _same_batch_fuzzy_merge(ideas)
    ideas = _filter_low_quality_tight_tp_ideas(ideas)
    ideas.sort(key=lambda x: x.get("score", 0), reverse=True)
    return ideas


def diagnose_trade_idea_flow(skinny_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return lightweight diagnostics for why trade ideas may be empty.
    Safe/pure helper: no mutation, no I/O.
    """
    market = _get_market(skinny_snapshot)
    stats: Dict[str, Any] = {
        "symbol": market.get("symbol"),
        "evaluated_tf_slots": 0,
        "missing_tf_slots": 0,
        "stale_tf_slots": 0,
        "fresh_tf_slots": 0,
        "trade_ideas_count": 0,
        "target_sources_by_tf": {},
        "targetless_tf_slots": 0,
    }

    for horizon, tfs in HORIZONS.items():
        for tf in tfs:
            stats["evaluated_tf_slots"] += 1
            tf_data = _tf(market, tf)
            if not tf_data:
                stats["missing_tf_slots"] += 1
                continue
            if tf_data.get("is_stale"):
                stats["stale_tf_slots"] += 1
                continue

            stats["fresh_tf_slots"] += 1
            target_probe_long = _build_targets(
                market=market,
                tf_data=tf_data,
                direction="long",
                horizon=horizon,
            )
            target_probe_short = _build_targets(
                market=market,
                tf_data=tf_data,
                direction="short",
                horizon=horizon,
            )

            long_target_count = len(target_probe_long.get("targets") or [])
            short_target_count = len(target_probe_short.get("targets") or [])
            stats["target_sources_by_tf"][tf] = {
                "horizon": horizon,
                "long_targets": long_target_count,
                "short_targets": short_target_count,
            }
            if long_target_count == 0 and short_target_count == 0:
                stats["targetless_tf_slots"] += 1

            candidates = _continuation_ideas(market, horizon, tf, tf_data)
            candidates.extend(_reversal_ideas(market, horizon, tf, tf_data))
            stats["trade_ideas_count"] += len(candidates)

    return stats
