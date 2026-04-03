import datetime as dt
import hashlib
from collections import deque
from typing import Any, Dict, List, Optional, Tuple



# Logging disabled outside sim_worker per repository policy.
def print(*args, **kwargs):  # type: ignore[override]
    return None

# ------------------------------------------------------------------
# Diagnostics
# ------------------------------------------------------------------
_diag_calls_total = 0
_diag_last5_spot_rows = deque(maxlen=5)
_diag_last_output = None
_LAST_LIQ_POOL_OUTPUT = None


# ---------------------------------------------------------------------------
# Liquidity Pool Builder (v1)
#
# Design goals:
# - Candle-driven, trading-hours agnostic
# - Uses existing spot_tf liquidity outputs (clean highs/lows, equal highs/lows)
# - Adds day/week extremes computed from whatever candles CandleEngine has
# - Produces a single ranked, de-duplicated pool (highs + lows)
# - Outputs are stored as a current snapshot only (DB upsert)
# ---------------------------------------------------------------------------


TF_BONUS: Dict[str, int] = {
    "1w": 20,
    "1d": 16,
    "4h": 12,
    "1h": 9,
    "15m": 6,
    "5m": 4,
    "3m": 2,
    "1m": 2,
}


BASE_SOURCE_SCORE: Dict[str, int] = {
    "week_high": 70,
    "week_low": 70,
    "day_high": 60,
    "day_low": 60,
    "equal_high_cluster": 55,
    "equal_low_cluster": 55,
    "swing_high": 45,
    "swing_low": 45,
}


POOL_VERSION = "liq_pool_v1"


def _parse_ts(ts: Any) -> Optional[dt.datetime]:
    if ts is None:
        return None
    if isinstance(ts, dt.datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=dt.timezone.utc)
    if isinstance(ts, str):
        try:
            t = dt.datetime.fromisoformat(ts)
            return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)
        except Exception:
            return None
    return None


def _tf_minutes(tf: str) -> int:
    tf = (tf or "").strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    if tf.endswith("d"):
        return int(tf[:-1]) * 1440
    if tf.endswith("w"):
        return int(tf[:-1]) * 10080
    return 10**9


def _tf_rank(tf: str) -> int:
    """Higher TF should rank higher."""
    return _tf_minutes(tf)


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _deterministic_id(
    symbol: str,
    kind: str,
    source_type: str,
    source_tf: str,
    ts_ref: Optional[str],
    level: Optional[float] = None,
    zone: Optional[Tuple[float, float]] = None,
) -> str:
    base = f"{symbol}|{kind}|{source_type}|{source_tf}|{ts_ref or ''}|"
    if level is not None:
        base += f"L:{level:.6f}"
    elif zone is not None:
        base += f"Z:{zone[0]:.6f}-{zone[1]:.6f}"
    else:
        base += "NA"
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:14]
    return f"{symbol}_{kind}_{source_type}_{source_tf}_{h}"


def _compute_tol_price(
    atr_5m: Optional[float],
    tick_size: float,
    min_tick_tol_ticks: int,
    atr_factor: float,
) -> float:
    min_tick_tol = float(min_tick_tol_ticks) * float(tick_size)
    atr_term = float(atr_factor) * float(atr_5m or 0.0)
    return max(min_tick_tol, atr_term)


def _recency_bonus(ts_ref: Optional[str], asof_dt: dt.datetime, source_tf: str) -> int:
    # Simple v1: <=1 day => +6, <=3 days => +3, else 0.
    # For weekly sources we keep 0 to avoid inflating.
    if source_tf == "1w":
        return 0
    t = _parse_ts(ts_ref)
    if not t:
        return 0
    age = asof_dt - t
    days = age.total_seconds() / 86400.0
    if days <= 1.0:
        return 6
    if days <= 3.0:
        return 3
    return 0


def _base_strength(source_type: str, source_tf: str, ts_ref: Optional[str], asof_dt: dt.datetime) -> float:
    base = float(BASE_SOURCE_SCORE.get(source_type, 40))
    tf_bonus = float(TF_BONUS.get(source_tf, 0))
    rec = float(_recency_bonus(ts_ref, asof_dt, source_tf))
    return base + tf_bonus + rec


def _level_center(item: Dict[str, Any]) -> float:
    if item.get("level") is not None:
        return float(item["level"])
    zl = item.get("zone_low")
    zh = item.get("zone_high")
    if zl is None or zh is None:
        return 0.0
    return (float(zl) + float(zh)) / 2.0


def _merge_items(primary: Dict[str, Any], other: Dict[str, Any]) -> Dict[str, Any]:
    # Keep higher TF as primary TF.
    if _tf_rank(other.get("source_tf", "")) > _tf_rank(primary.get("source_tf", "")):
        primary, other = other, primary

    # Merge sources list (unique)
    srcs = list(primary.get("sources") or [])
    for s in (other.get("sources") or []):
        if s not in srcs:
            srcs.append(s)
    # Also capture other source_type explicitly for traceability
    ot = other.get("source_type")
    if ot and ot not in srcs:
        srcs.append(ot)

    primary["sources"] = srcs

    # Confluence bonus: +6 per additional unique source beyond first, cap +18
    n_sources = max(1, len(srcs))
    conf_bonus = min(18, 6 * (n_sources - 1))
    base_strength = float(primary.get("_strength_raw", 0.0))
    primary["_strength_raw"] = base_strength + conf_bonus

    # Expand zone if either item had a zone
    if primary.get("zone_low") is not None or other.get("zone_low") is not None:
        zl = min(
            float(primary.get("zone_low", _level_center(primary))),
            float(other.get("zone_low", _level_center(other))),
        )
        zh = max(
            float(primary.get("zone_high", _level_center(primary))),
            float(other.get("zone_high", _level_center(other))),
        )
        primary["zone_low"], primary["zone_high"] = zl, zh

    return primary


def build_liquidity_pool(
    symbol: str,
    spot_tf_rows: List[Dict[str, Any]],
    candle_engine: Any,
    *,
    tick_size: float = 0.01,
    min_tick_tol_ticks: int = 3,
    atr_factor: float = 0.10,
    cap_per_side: int = 50,
) -> Dict[str, Any]:
    """Build the current liquidity pool snapshot for a symbol.

    Returns:
      {
        "symbol": str,
        "asof": iso str,
        "pool_version": str,
        "levels": [ ... ],
        "stats": { ... }
      }
    """
    global _diag_calls_total, _diag_last5_spot_rows, _diag_last_output, _LAST_LIQ_POOL_OUTPUT

    _diag_calls_total += 1
    _diag_last5_spot_rows.append(len(spot_tf_rows or []))

    sym = (symbol or "").upper()

    # asof: use candle_engine latest candle ts if present, else spot_tf latest
    asof_dt: dt.datetime = dt.datetime.now(dt.timezone.utc)
    try:
        # Find the smallest TF we have candles for, take its last closed candle ts
        sym_candles = (getattr(candle_engine, "candles", {}) or {}).get(sym, {})
        best_tf = None
        best_minutes = 10**9
        for tf, cands in (sym_candles or {}).items():
            if not cands:
                continue
            m = _tf_minutes(tf)
            if m < best_minutes:
                best_minutes = m
                best_tf = tf
        if best_tf:
            last = sym_candles[best_tf][-1]
            t = _parse_ts(last.get("ts"))
            if t:
                asof_dt = t
    except Exception:
        pass

    # ATR_5m (for dedupe tolerance) from spot_tf 5m extras
    atr_5m: Optional[float] = None
    for r in spot_tf_rows or []:
        if (r.get("timeframe") or "").lower() == "5m":
            extras = r.get("extras") or {}
            try:
                atr_5m = float(extras.get("atr")) if extras.get("atr") is not None else None
            except Exception:
                atr_5m = None
            break

    tol_price = _compute_tol_price(atr_5m, tick_size, min_tick_tol_ticks, atr_factor)

    candidates: List[Dict[str, Any]] = []

    # --- A) Swings via clean highs/lows from liquidity indicator (state == active) ---
    for r in spot_tf_rows or []:
        tf = (r.get("timeframe") or "").lower()
        liq = r.get("liquidity") or {}
        for ch in (liq.get("clean_highs") or []):
            if (ch.get("state") or "").lower() != "active":
                continue
            price = ch.get("price")
            if price is None:
                continue
            ts_ref = ch.get("ts")
            item = {
                "kind": "high",
                "level": float(price),
                "source_type": "swing_high",
                "source_tf": tf,
                "ts_ref": ts_ref,
                "sources": ["swing_high"],
            }
            item["id"] = _deterministic_id(sym, "high", "swing_high", tf, str(ts_ref), level=float(price))
            item["_strength_raw"] = _base_strength("swing_high", tf, str(ts_ref), asof_dt)
            candidates.append(item)

        for cl in (liq.get("clean_lows") or []):
            if (cl.get("state") or "").lower() != "active":
                continue
            price = cl.get("price")
            if price is None:
                continue
            ts_ref = cl.get("ts")
            item = {
                "kind": "low",
                "level": float(price),
                "source_type": "swing_low",
                "source_tf": tf,
                "ts_ref": ts_ref,
                "sources": ["swing_low"],
            }
            item["id"] = _deterministic_id(sym, "low", "swing_low", tf, str(ts_ref), level=float(price))
            item["_strength_raw"] = _base_strength("swing_low", tf, str(ts_ref), asof_dt)
            candidates.append(item)

        # --- B) Equal highs/lows clusters (state == active) ---
        for eh in (liq.get("equal_highs") or []):
            if (eh.get("state") or "").lower() != "active":
                continue
            price = eh.get("price")
            if price is None:
                continue
            ts_ref = eh.get("formed_ts") or eh.get("ts")
            p = float(price)
            zl, zh = p - (tol_price / 2.0), p + (tol_price / 2.0)
            item = {
                "kind": "high",
                "level": p,
                "zone_low": zl,
                "zone_high": zh,
                "source_type": "equal_high_cluster",
                "source_tf": tf,
                "ts_ref": ts_ref,
                "sources": ["equal_high_cluster"],
            }
            item["id"] = _deterministic_id(sym, "high", "equal_high_cluster", tf, str(ts_ref), level=p)
            item["_strength_raw"] = _base_strength("equal_high_cluster", tf, str(ts_ref), asof_dt)
            candidates.append(item)

        for el in (liq.get("equal_lows") or []):
            if (el.get("state") or "").lower() != "active":
                continue
            price = el.get("price")
            if price is None:
                continue
            ts_ref = el.get("formed_ts") or el.get("ts")
            p = float(price)
            zl, zh = p - (tol_price / 2.0), p + (tol_price / 2.0)
            item = {
                "kind": "low",
                "level": p,
                "zone_low": zl,
                "zone_high": zh,
                "source_type": "equal_low_cluster",
                "source_tf": tf,
                "ts_ref": ts_ref,
                "sources": ["equal_low_cluster"],
            }
            item["id"] = _deterministic_id(sym, "low", "equal_low_cluster", tf, str(ts_ref), level=p)
            item["_strength_raw"] = _base_strength("equal_low_cluster", tf, str(ts_ref), asof_dt)
            candidates.append(item)

    # --- C) Day/Week extremes from candle_engine (date-bucket, hours agnostic) ---
    try:
        sym_candles = (getattr(candle_engine, "candles", {}) or {}).get(sym, {})
        # pick smallest tf available to approximate intraday extremes
        best_tf = None
        best_minutes = 10**9
        for tf, cands in (sym_candles or {}).items():
            if not cands:
                continue
            m = _tf_minutes(tf)
            if m < best_minutes:
                best_minutes = m
                best_tf = tf
        if best_tf:
            cands = sym_candles[best_tf]
            # Day bucket uses date_et if present, else UTC date.
            dates = [c.get("date_et") or (_parse_ts(c.get("ts")) or asof_dt).date().isoformat() for c in cands]
            last_date = dates[-1] if dates else None
            prev_dates = [d for d in dates if d != last_date]
            prev_date = prev_dates[-1] if prev_dates else None
            if prev_date:
                day_cands = [c for c, d in zip(cands, dates) if d == prev_date]
                if day_cands:
                    dh = max(float(c.get("high")) for c in day_cands if c.get("high") is not None)
                    dl = min(float(c.get("low")) for c in day_cands if c.get("low") is not None)
                    ts_ref = (day_cands[-1].get("ts") or None)

                    hi = {
                        "kind": "high",
                        "level": dh,
                        "source_type": "day_high",
                        "source_tf": "1d",
                        "ts_ref": ts_ref,
                        "sources": ["day_high"],
                    }
                    hi["id"] = _deterministic_id(sym, "high", "day_high", "1d", str(ts_ref), level=dh)
                    hi["_strength_raw"] = _base_strength("day_high", "1d", str(ts_ref), asof_dt)
                    candidates.append(hi)

                    lo = {
                        "kind": "low",
                        "level": dl,
                        "source_type": "day_low",
                        "source_tf": "1d",
                        "ts_ref": ts_ref,
                        "sources": ["day_low"],
                    }
                    lo["id"] = _deterministic_id(sym, "low", "day_low", "1d", str(ts_ref), level=dl)
                    lo["_strength_raw"] = _base_strength("day_low", "1d", str(ts_ref), asof_dt)
                    candidates.append(lo)

            # Week bucket: use ISO week of UTC timestamp
            ts_list = [_parse_ts(c.get("ts")) or asof_dt for c in cands]
            weeks = [f"{t.isocalendar().year}-W{t.isocalendar().week:02d}" for t in ts_list]
            last_week = weeks[-1] if weeks else None
            prev_weeks = [w for w in weeks if w != last_week]
            prev_week = prev_weeks[-1] if prev_weeks else None
            if prev_week:
                wk_cands = [c for c, w in zip(cands, weeks) if w == prev_week]
                if wk_cands:
                    wh = max(float(c.get("high")) for c in wk_cands if c.get("high") is not None)
                    wl = min(float(c.get("low")) for c in wk_cands if c.get("low") is not None)
                    ts_ref = (wk_cands[-1].get("ts") or None)

                    hi = {
                        "kind": "high",
                        "level": wh,
                        "source_type": "week_high",
                        "source_tf": "1w",
                        "ts_ref": ts_ref,
                        "sources": ["week_high"],
                    }
                    hi["id"] = _deterministic_id(sym, "high", "week_high", "1w", str(ts_ref), level=wh)
                    hi["_strength_raw"] = _base_strength("week_high", "1w", str(ts_ref), asof_dt)
                    candidates.append(hi)

                    lo = {
                        "kind": "low",
                        "level": wl,
                        "source_type": "week_low",
                        "source_tf": "1w",
                        "ts_ref": ts_ref,
                        "sources": ["week_low"],
                    }
                    lo["id"] = _deterministic_id(sym, "low", "week_low", "1w", str(ts_ref), level=wl)
                    lo["_strength_raw"] = _base_strength("week_low", "1w", str(ts_ref), asof_dt)
                    candidates.append(lo)
    except Exception:
        # Don't fail the whole pipeline if extremes can't be computed.
        pass

    # --- De-dupe (same-kind only) using tol_price ---
    highs = [c for c in candidates if c.get("kind") == "high"]
    lows = [c for c in candidates if c.get("kind") == "low"]

    def _dedupe(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Sort by preliminary strength, then TF, then recency
        items_sorted = sorted(
            items,
            key=lambda x: (
                float(x.get("_strength_raw", 0.0)),
                _tf_rank(x.get("source_tf", "")),
                _parse_ts(x.get("ts_ref")) or dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc),
            ),
            reverse=True,
        )
        accepted: List[Dict[str, Any]] = []
        for it in items_sorted:
            center = _level_center(it)
            merged = False
            for j in range(len(accepted)):
                a = accepted[j]
                if abs(_level_center(a) - center) <= tol_price:
                    accepted[j] = _merge_items(a, it)
                    merged = True
                    break
            if not merged:
                accepted.append(it)
        return accepted

    highs_d = _dedupe(highs)
    lows_d = _dedupe(lows)

    # Finalize strength (clamp + remove internals)
    def _finalize(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for it in items:
            s = _clamp(float(it.get("_strength_raw", 0.0)))
            it["strength"] = round(float(s), 2)
            it.pop("_strength_raw", None)
            out.append(it)
        return out

    highs_f = _finalize(highs_d)
    lows_f = _finalize(lows_d)

    # Rank and cap per side
    def _sort_key(it: Dict[str, Any]):
        return (
            float(it.get("strength", 0.0)),
            _tf_rank(it.get("source_tf", "")),
            _parse_ts(it.get("ts_ref")) or dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc),
        )

    highs_f = sorted(highs_f, key=_sort_key, reverse=True)[:cap_per_side]
    lows_f = sorted(lows_f, key=_sort_key, reverse=True)[:cap_per_side]

    levels = highs_f + lows_f

    stats: Dict[str, Any] = {
        "count_total": len(levels),
        "count_high": len(highs_f),
        "count_low": len(lows_f),
        "by_tf": {},
        "by_type": {},
    }
    for it in levels:
        tf = it.get("source_tf")
        st = it.get("source_type")
        stats["by_tf"][tf] = int(stats["by_tf"].get(tf, 0)) + 1
        stats["by_type"][st] = int(stats["by_type"].get(st, 0)) + 1

    if levels:
        prices = [float(it.get("level")) for it in levels if it.get("level") is not None]
        if prices:
            stats["min_level"] = min(prices)
            stats["max_level"] = max(prices)

    result = {
        "symbol": sym,
        "asof": asof_dt.isoformat(),
        "pool_version": POOL_VERSION,
        "tol_price": tol_price,
        "levels": levels,
        "stats": stats,
    }

    # Store ONLY the last output; do NOT print per call.
    _diag_last_output = result
    _LAST_LIQ_POOL_OUTPUT = result

    return result


def get_last_liquidity_output():
    """Return the last liquidity pool output for callers that want to log it."""
    return _diag_last_output


def print_last_liquidity_output() -> None:
    """Print only the final (most recent) liquidity pool output once."""
    global _LAST_LIQ_POOL_OUTPUT
    # print(f"[LIQ_POOL][FINAL_OUTPUT] {_LAST_LIQ_POOL_OUTPUT}")
