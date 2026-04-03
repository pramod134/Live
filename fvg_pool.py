import datetime as dt
import json
import os
from typing import Any, Dict, List, Optional, Tuple

import httpx

_TF_ORDER: Dict[str, int] = {
    "1m": 1,
    "3m": 2,
    "5m": 3,
    "15m": 4,
    "30m": 5,
    "1h": 6,
    "4h": 7,
    "1d": 8,
    "1w": 9,
}

_TF_STRENGTH: Dict[str, int] = {
    "1d": 20,
    "4h": 18,
    "1h": 15,
    "30m": 12,
    "15m": 9,
    "5m": 6,
    "3m": 4,
    "1m": 2,
}


def _tf_rank(tf: Optional[str]) -> int:
    if not tf:
        return 0
    return _TF_ORDER.get(str(tf).strip().lower(), 0)


def _sort_timeframes(tfs: List[str]) -> List[str]:
    return sorted(tfs, key=_tf_rank)


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        f = float(value)
        if f != f:
            return None
        return f
    except Exception:
        return None


def _parse_ts(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc).isoformat()
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.astimezone(dt.timezone.utc).isoformat()
        except Exception:
            return s
    return None


def _parse_direction(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    d = str(raw).strip().lower()
    if d in {"bull", "bullish", "buy", "long"}:
        return "bull"
    if d in {"bear", "bearish", "sell", "short"}:
        return "bear"
    return None


def _is_fully_filled(raw: Dict[str, Any]) -> bool:
    filled = raw.get("filled")
    if isinstance(filled, bool) and filled:
        return True
    filled_pct = _to_float(raw.get("filled_pct"))
    return filled_pct is not None and filled_pct >= 100.0


def _zone_distance(a_low: float, a_high: float, b_low: float, b_high: float) -> float:
    if a_high >= b_low and b_high >= a_low:
        return 0.0
    if a_high < b_low:
        return b_low - a_high
    return a_low - b_high


def _mergeable(a: Dict[str, Any], b: Dict[str, Any], pct_tolerance: float) -> bool:
    dist = _zone_distance(a["zone_low"], a["zone_high"], b["zone_low"], b["zone_high"])
    avg_mid = ((a["mid"] + b["mid"]) / 2.0) if (a.get("mid") is not None and b.get("mid") is not None) else 0.0
    tol = abs(avg_mid) * (pct_tolerance / 100.0)
    return dist <= tol


def _extract_current_price(fvg_rows: List[Dict[str, Any]]) -> Optional[float]:
    candidates: List[Tuple[int, float]] = []
    for row in fvg_rows:
        tf_rank = _tf_rank(row.get("timeframe"))
        close_val = _to_float(row.get("current_price"))
        if close_val is not None:
            candidates.append((tf_rank, close_val))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _compute_freshness_component(now: dt.datetime, members: List[Dict[str, Any]]) -> int:
    age_vals = [_to_float(m.get("age_minutes")) for m in members]
    age_vals = [a for a in age_vals if a is not None and a >= 0]
    freshest_age_minutes: Optional[float] = min(age_vals) if age_vals else None

    if freshest_age_minutes is None:
        parsed_created = []
        for m in members:
            ts = _parse_ts(m.get("created_ts"))
            if not ts:
                continue
            try:
                parsed_created.append(dt.datetime.fromisoformat(ts.replace("Z", "+00:00")))
            except Exception:
                continue
        if parsed_created:
            freshest = max(parsed_created)
            freshest_age_minutes = max(0.0, (now - freshest).total_seconds() / 60.0)

    if freshest_age_minutes is None:
        return 3
    if freshest_age_minutes < 30:
        return 10
    if freshest_age_minutes < 120:
        return 8
    if freshest_age_minutes < 1440:
        return 6
    if freshest_age_minutes < 4320:
        return 4
    return 2


def _compute_cleanliness_component(members: List[Dict[str, Any]]) -> int:
    touches = [_to_float(m.get("touch_count")) for m in members]
    touches = [t for t in touches if t is not None]
    if not touches:
        return 7
    avg_touch = sum(touches) / max(1, len(touches))
    if avg_touch <= 0:
        return 10
    if avg_touch <= 1:
        return 9
    if avg_touch <= 2:
        return 8
    if avg_touch <= 3:
        return 6
    if avg_touch <= 5:
        return 4
    return 2


def _build_pool_from_members(
    symbol: str,
    direction: str,
    members: List[Dict[str, Any]],
    idx: int,
    current_price: Optional[float],
) -> Dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc)
    lows = [m["zone_low"] for m in members]
    highs = [m["zone_high"] for m in members]

    envelope_low = min(lows)
    envelope_high = max(highs)
    mid = (envelope_low + envelope_high) / 2.0

    core_low = max(lows)
    core_high = min(highs)
    core_zone: Optional[Dict[str, float]] = None
    if core_low < core_high:
        core_zone = {
            "zone_low": core_low,
            "zone_high": core_high,
            "mid": (core_low + core_high) / 2.0,
        }
    else:
        core_low = None  # type: ignore[assignment]
        core_high = None  # type: ignore[assignment]

    source_tfs = sorted({str(m.get("source_tf")) for m in members if m.get("source_tf")}, key=_tf_rank)
    anchor_tf = source_tfs[-1] if source_tfs else None

    filled_pcts = [_to_float(m.get("filled_pct")) for m in members]
    filled_pcts_clean = [f for f in filled_pcts if f is not None]

    touch_vals = [_to_float(m.get("touch_count")) for m in members]
    touch_clean = [t for t in touch_vals if t is not None]

    created_all = [_parse_ts(m.get("created_ts")) for m in members]
    created_clean = [c for c in created_all if c is not None]

    quality_values = []
    for m in members:
        trade_score = _to_float(m.get("trade_score"))
        fvg_score = _to_float(m.get("fvg_score"))
        quality_values.append(trade_score if trade_score is not None else fvg_score)
    quality_values = [q for q in quality_values if q is not None]
    strongest_quality = max(quality_values) if quality_values else 0.0
    quality_component = min(40.0, strongest_quality * 0.40)

    timeframe_component = _TF_STRENGTH.get((anchor_tf or "").lower(), 0)

    tf_count = len(source_tfs)
    if tf_count <= 1:
        mtf_component = 5
    elif tf_count == 2:
        mtf_component = 10
    elif tf_count == 3:
        mtf_component = 15
    else:
        mtf_component = 20

    freshness_component = _compute_freshness_component(now, members)
    cleanliness_component = _compute_cleanliness_component(members)

    pooled_score = max(0.0, min(100.0, quality_component + timeframe_component + mtf_component + freshness_component + cleanliness_component))

    has_mitigated_member = any((_to_float(m.get("filled_pct")) or 0.0) > 0 for m in members)
    status = "mitigated" if has_mitigated_member else "active"

    distance_from_price = None
    distance_from_price_pct = None
    location_vs_price = None
    if current_price is not None:
        if envelope_low <= current_price <= envelope_high:
            distance_from_price = 0.0
            distance_from_price_pct = 0.0
            location_vs_price = "contains"
        elif current_price < envelope_low:
            distance_from_price = envelope_low - current_price
            location_vs_price = "above"
        else:
            distance_from_price = current_price - envelope_high
            location_vs_price = "below"

        if distance_from_price is not None and current_price != 0:
            distance_from_price_pct = (distance_from_price / abs(current_price)) * 100.0

    trade_scores = [_to_float(m.get("trade_score")) for m in members]
    trade_scores = [s for s in trade_scores if s is not None]
    fvg_scores = [_to_float(m.get("fvg_score")) for m in members]
    fvg_scores = [s for s in fvg_scores if s is not None]

    pool = {
        "pool_id": f"{symbol}:{direction}:{idx}",
        "symbol": symbol,
        "direction": direction,
        "status": status,
        "pool_type": "stacked_pool" if tf_count >= 2 else "standard_pool",
        "zone_low": envelope_low,
        "zone_high": envelope_high,
        "envelope_low": envelope_low,
        "envelope_high": envelope_high,
        "core_low": core_low,
        "core_high": core_high,
        "core_zone": core_zone,
        "mid": mid,
        "source_tfs": source_tfs,
        "anchor_tf": anchor_tf,
        "member_count": len(members),
        "members": members,
        "best_trade_score": max(trade_scores) if trade_scores else None,
        "best_fvg_score": max(fvg_scores) if fvg_scores else None,
        "avg_touch_count": (sum(touch_clean) / len(touch_clean)) if touch_clean else None,
        "min_filled_pct": min(filled_pcts_clean) if filled_pcts_clean else None,
        "max_filled_pct": max(filled_pcts_clean) if filled_pcts_clean else None,
        "avg_filled_pct": (sum(filled_pcts_clean) / len(filled_pcts_clean)) if filled_pcts_clean else None,
        "created_ts_min": min(created_clean) if created_clean else None,
        "created_ts_max": max(created_clean) if created_clean else None,
        "freshest_created_ts": max(created_clean) if created_clean else None,
        "oldest_created_ts": min(created_clean) if created_clean else None,
        "pooled_score": pooled_score,
        "distance_from_price": distance_from_price,
        "distance_from_price_pct": distance_from_price_pct,
        "location_vs_price": location_vs_price,
    }
    return pool


def _cluster_members(items: List[Dict[str, Any]], pct_tolerance: float) -> List[List[Dict[str, Any]]]:
    clusters: List[List[Dict[str, Any]]] = []
    for item in sorted(items, key=lambda x: (x["zone_low"], x["zone_high"])):
        matched_idx: List[int] = []
        for i, cluster in enumerate(clusters):
            if any(_mergeable(item, existing, pct_tolerance) for existing in cluster):
                matched_idx.append(i)

        if not matched_idx:
            clusters.append([item])
            continue

        primary = matched_idx[0]
        clusters[primary].append(item)
        for merge_idx in reversed(matched_idx[1:]):
            clusters[primary].extend(clusters[merge_idx])
            del clusters[merge_idx]
    return clusters


def build_symbol_fvg_pool(
    *,
    symbol: str,
    fvg_rows: List[Dict[str, Any]],
    current_price: Optional[float] = None,
    timeframes_used: Optional[List[str]] = None,
) -> Dict[str, Any]:
    symbol_norm = str(symbol or "").upper()
    selected_tfs = set(tf.lower() for tf in (timeframes_used or []) if isinstance(tf, str) and tf.strip())

    raw_fvg_count = 0
    invalid_discarded_count = 0
    filled_excluded_count = 0

    normalized: List[Dict[str, Any]] = []
    used_tfs_set = set()

    for row in fvg_rows or []:
        tf = row.get("timeframe")
        if not isinstance(tf, str) or not tf.strip():
            continue
        tf_clean = tf.strip().lower()
        if selected_tfs and tf_clean not in selected_tfs:
            continue

        used_tfs_set.add(tf_clean)
        fvgs = row.get("fvgs")
        if not isinstance(fvgs, list):
            continue

        for raw in fvgs:
            raw_fvg_count += 1
            if not isinstance(raw, dict):
                invalid_discarded_count += 1
                continue

            direction = _parse_direction(raw.get("direction"))
            low = _to_float(raw.get("low"))
            high = _to_float(raw.get("high"))

            if direction is None or low is None or high is None:
                invalid_discarded_count += 1
                continue

            zone_low = min(low, high)
            zone_high = max(low, high)
            if zone_high <= zone_low:
                invalid_discarded_count += 1
                continue

            if _is_fully_filled(raw):
                filled_excluded_count += 1
                continue

            normalized.append(
                {
                    "symbol": symbol_norm,
                    "source_tf": tf_clean,
                    "direction": direction,
                    "zone_low": zone_low,
                    "zone_high": zone_high,
                    "mid": (zone_low + zone_high) / 2.0,
                    "created_ts": _parse_ts(raw.get("created_ts")),
                    "impulse_ts": _parse_ts(raw.get("impulse_ts")),
                    "filled": raw.get("filled"),
                    "filled_pct": _to_float(raw.get("filled_pct")),
                    "touch_count": _to_float(raw.get("touch_count")),
                    "fvg_score": _to_float(raw.get("fvg_score")),
                    "trade_score": _to_float(raw.get("trade_score")),
                    "age_minutes": _to_float(raw.get("age_minutes")),
                    "style": raw.get("style"),
                    "context": raw.get("context"),
                    "first_touch": raw.get("first_touch"),
                    "last_touch": raw.get("last_touch"),
                    "raw": raw,
                }
            )

    if current_price is None:
        current_price = _extract_current_price(fvg_rows or [])
    pct_tolerance = 0.10

    pools_by_direction: Dict[str, List[Dict[str, Any]]] = {"bull": [], "bear": []}
    for direction in ("bull", "bear"):
        direction_items = [x for x in normalized if x["direction"] == direction]
        clusters = _cluster_members(direction_items, pct_tolerance=pct_tolerance)
        built = [
            _build_pool_from_members(
                symbol=symbol_norm,
                direction=direction,
                members=cluster,
                idx=i + 1,
                current_price=current_price,
            )
            for i, cluster in enumerate(clusters)
            if cluster
        ]
        pools_by_direction[direction] = built

    def _sort_key(pool: Dict[str, Any]) -> Tuple[float, int, int]:
        tfs = pool.get("source_tfs") or []
        top_tf_rank = max((_tf_rank(tf) for tf in tfs), default=0)
        return (
            float(pool.get("pooled_score") or 0.0),
            top_tf_rank,
            int(pool.get("member_count") or 0),
        )

    bullish_pools = sorted(pools_by_direction["bull"], key=_sort_key, reverse=True)
    bearish_pools = sorted(pools_by_direction["bear"], key=_sort_key, reverse=True)

    out = {
        "symbol": symbol_norm,
        "asof_ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        "rules_version": "fvg_pool_v1",
        "timeframes_used": _sort_timeframes(list(used_tfs_set)),
        "current_price": current_price,
        "bullish_pools": bullish_pools,
        "bearish_pools": bearish_pools,
        "meta": {
            "raw_fvg_count": raw_fvg_count,
            "invalid_discarded_count": invalid_discarded_count,
            "filled_excluded_count": filled_excluded_count,
            "eligible_fvg_count": len(normalized),
            "bullish_pool_count": len(bullish_pools),
            "bearish_pool_count": len(bearish_pools),
            "merge_tolerance_mode": "pct",
            "merge_tolerance_value": pct_tolerance,
        },
    }
    return out


async def upsert_symbol_fvg_pool(
    *,
    fvg_pool: Dict[str, Any],
    table: str = "fvg_pool",
    supabase_url: Optional[str] = None,
    supabase_key: Optional[str] = None,
    timeout_s: float = 20.0,
) -> None:
    url = supabase_url or os.getenv("SUPABASE_URL")
    key = supabase_key or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

    if not url or not key:
        return

    endpoint = f"{url.rstrip('/')}/rest/v1/{table}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    params = {"on_conflict": "symbol"}

    payload = {
        "symbol": fvg_pool.get("symbol"),
        "asof_ts": fvg_pool.get("asof_ts"),
        "rules_version": fvg_pool.get("rules_version"),
        "timeframes_used": fvg_pool.get("timeframes_used"),
        "current_price": fvg_pool.get("current_price"),
        "bullish_pools": fvg_pool.get("bullish_pools"),
        "bearish_pools": fvg_pool.get("bearish_pools"),
        "meta": fvg_pool.get("meta"),
    }

    async with httpx.AsyncClient(timeout=timeout_s) as client:
        response = await client.post(endpoint, params=params, json=payload, headers=headers)
        response.raise_for_status()


def format_fvg_pool_for_log(fvg_pool: Dict[str, Any]) -> str:
    return json.dumps(fvg_pool, indent=2, default=str)
