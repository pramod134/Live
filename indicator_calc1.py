import math
from typing import Any, Dict, List, Optional, Tuple

import datetime as dt

_LAST_PRINTED_LIVE_POINT: Optional[Tuple[str, str, str, float, Optional[str]]] = None

def _parse_ts(ts):
    if not isinstance(ts, str):
        return None
    try:
        return dt.datetime.fromisoformat(ts)
    except Exception:
        return None

def _candle_dir(c):
    o = float(c.get("open", 0))
    cl = float(c.get("close", 0))
    if cl > o:
        return "bull"
    if cl < o:
        return "bear"
    return "doji"

def _touch_behavior(c, zone_low, zone_high):
    h = float(c["high"])
    l = float(c["low"])
    cl = float(c["close"])
    overlaps = not (h < zone_low or l > zone_high)
    close_location = "outside" if (cl < zone_low or cl > zone_high) else "inside"
    return {
        "ts": c.get("ts"),
        "candle_dir": _candle_dir(c),
        "close_location": close_location,
        "rejection": overlaps and close_location == "outside",
    }


def _net_change_pct(window):
    if not window:
        return None
    try:
        first_open = float(window[0].get("open", 0.0))
        last_close = float(window[-1].get("close", 0.0))
        if first_open == 0.0:
            return None
        return (last_close - first_open) / first_open * 100.0
    except Exception:
        return None

def _dir_by_counts(bull_count, bear_count):
    if bull_count > bear_count:
        return "bullish"
    if bear_count > bull_count:
        return "bearish"
    return "neutral"

def _dir_by_drift(net_change_pct, eps=0.02):
    if net_change_pct is None:
        return "neutral"
    if net_change_pct > eps:
        return "bullish"
    if net_change_pct < -eps:
        return "bearish"
    return "neutral"

def _alignment_score(ref_dir, by_counts, by_drift):
    # ref_dir is "bullish" or "bearish"
    score = 0.5
    opp = "bearish" if ref_dir == "bullish" else "bullish"

    if by_counts == ref_dir:
        score += 0.25
    elif by_counts == opp:
        score -= 0.25

    if by_drift == ref_dir:
        score += 0.25
    elif by_drift == opp:
        score -= 0.25

    return _clamp(score, 0.0, 1.0)

def _norm_body_vs_atr(body_vs_atr):
    if body_vs_atr is None:
        return 0.0
    # 0 -> 0, 2 ATR body -> 1
    return _clamp(body_vs_atr / 2.0, 0.0, 1.0)

def _norm_vol_rel(vol_rel):
    if vol_rel is None:
        return 0.0
    # 1.0 baseline -> 0, 2.0 strong -> 1
    return _clamp((vol_rel - 1.0) / 1.0, 0.0, 1.0)

def _trade_score_final(filled, filled_pct, touch_count, last_touch):
    if filled:
        return 0.0

    # 60 pts mitigation (piecewise curve we finalized)
    m = float(filled_pct or 0.0)
    if m <= 10:
        mit = 60.0 - (m / 10.0) * 10.0  # 60 -> 50
    elif m <= 40:
        mit = 50.0 - ((m - 10.0) / 30.0) * 25.0  # 50 -> 25
    elif m <= 80:
        mit = 25.0 - ((m - 40.0) / 40.0) * 20.0  # 25 -> 5
    else:
        mit = 5.0 - ((m - 80.0) / 20.0) * 5.0  # 5 -> 0
    mit = _clamp(mit, 0.0, 60.0)

    # 25 pts touch_count mapping
    if touch_count <= 0:
        tpts = 25.0
    elif touch_count == 1:
        tpts = 22.0
    elif touch_count == 2:
        tpts = 15.0
    elif touch_count == 3:
        tpts = 7.0
    else:
        tpts = 0.0

    # 15 pts last touch behavior
    if not last_touch:
        lpts = 10.0
    else:
        rej = bool(last_touch.get("rejection"))
        loc = last_touch.get("close_location")
        if rej:
            lpts = 15.0
        elif loc == "outside":
            lpts = 12.0
        elif loc == "inside":
            lpts = 6.0
        else:
            lpts = 0.0

    return float(_clamp(mit + tpts + lpts, 0.0, 100.0))




def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _avg(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    return sum(vals) / float(len(vals))


def _mini_candle(c: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not c:
        return None
    return {
        "ts": c.get("ts"),
        "tf": c.get("tf") or c.get("timeframe"),
        "ts_et": c.get("ts_et"),
        "date_et": c.get("date_et"),
        "time_et": c.get("time_et"),
        "open": float(c.get("open", 0.0)),
        "high": float(c.get("high", 0.0)),
        "low": float(c.get("low", 0.0)),
        "close": float(c.get("close", 0.0)),
        "volume": float(c.get("volume", 0.0)),
        "direction": c.get("direction"),
        "shape": c.get("shape"),
        "body": c.get("body"),
        "range": c.get("range"),
        "upper_wick": c.get("upper_wick"),
        "lower_wick": c.get("lower_wick"),
        "vol_rel": c.get("vol_rel"),
        "mom_raw": c.get("mom_raw"),
        "mom_atr": c.get("mom_atr"),
        "spread_strength": c.get("spread_strength"),
        "cluster": c.get("cluster"),
    }







def _ema(values: List[float], period: int) -> Optional[float]:
    """
    Simple EMA implementation on a list of values.
    Uses first value as seed; good enough for v1.
    """
    if not values:
        return None
    k = 2.0 / (period + 1.0)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1.0 - k)
    return ema


def _compute_atr(candles: List[Dict[str, Any]], period: int = 14) -> Optional[float]:
    """
    Compute a basic ATR from enriched candles (using high/low/close).
    """
    if len(candles) < 2:
        return None

    trs: List[float] = []
    start_idx = max(1, len(candles) - period)
    for i in range(start_idx, len(candles)):
        cur = candles[i]
        prev = candles[i - 1]
        h = float(cur["high"])
        l = float(cur["low"])
        c_prev = float(prev["close"])
        tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
        trs.append(tr)

    if not trs:
        return None
    return sum(trs) / len(trs)





def _compute_volume_profile(
    candles: List[Dict[str, Any]],
    max_lookback: int = 300,
    *,
    bin_count: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Volume profile approximation over last N candles, with optional fixed bin_count.
    """
    if not candles:
        return {
            "asof": None,
            "window": 0,
            "poc": None,
            "bins": [],
            "start_ts": None,
            "end_ts": None,
            "bin_count": bin_count or 0,
        }

    tail = candles[-max_lookback:]
    start_ts = tail[0].get("ts") if tail else None
    end_ts = tail[-1].get("ts") if tail else None

    lows: List[float] = []
    highs: List[float] = []
    for c in tail:
        try:
            lows.append(float(c["low"]))
            highs.append(float(c["high"]))
        except Exception:
            continue

    if not lows or not highs:
        return {
            "asof": candles[-1].get("ts"),
            "window": 0,
            "poc": None,
            "bins": [],
            "start_ts": start_ts,
            "end_ts": end_ts,
            "bin_count": bin_count or 0,
        }

    p_min = min(lows)
    p_max = max(highs)
    window = len(tail)

    # Decide number of bins
    if bin_count is not None:
        bins_count = int(bin_count)
    else:
        bins_count = min(40, max(8, int(math.sqrt(window))))

    if bins_count <= 0:
        bins_count = 8

    if p_max <= p_min:
        total_vol = sum(float(c.get("volume", 0.0)) for c in tail)
        return {
            "asof": candles[-1].get("ts"),
            "window": window,
            "poc": p_min,
            "bins": [{"price": p_min, "volume": total_vol}],
            "start_ts": start_ts,
            "end_ts": end_ts,
            "bin_count": bins_count,
        }

    width = (p_max - p_min) / bins_count if bins_count > 0 else (p_max - p_min) or 1.0
    if width <= 0:
        width = (p_max - p_min) or 1.0

    bin_vols = [0.0] * bins_count
    inv_width = 1.0 / width if width != 0 else 0.0

    def _clamp_idx(i: int) -> int:
        if i < 0:
            return 0
        if i >= bins_count:
            return bins_count - 1
        return i

    def _idx_from_price(x: float) -> int:
        return _clamp_idx(int((x - p_min) * inv_width))

    def _bin_center(i: int) -> float:
        return p_min + (i + 0.5) * width

    for c in tail:
        try:
            o = float(c["open"])
            h = float(c["high"])
            l = float(c["low"])
            cl = float(c["close"])
            v = float(c.get("volume", 0.0))
        except Exception:
            continue

        if v <= 0.0:
            continue

        lo = min(l, h)
        hi = max(l, h)

        a = _idx_from_price(lo)
        b = _idx_from_price(hi)

        if a > b:
            a, b = b, a

        # ---------------------------
        # Gaussian 70% (Hybrid B)
        # ---------------------------
        vg = 0.70 * v

        vw = c.get("vw") or c.get("vwap")
        try:
            vw = float(vw)
        except Exception:
            vw = (hi + lo) * 0.5
        if (
            not math.isfinite(vw)
            or vw <= 0
            or vw < lo
            or vw > hi
        ):
            vw = (hi + lo) * 0.5

        sigma = max((hi - lo) / 3.0, width)
        if sigma <= 0:
            sigma = width

        denom = 2.0 * sigma * sigma

        weights = []
        idxs = []

        for i in range(a, b + 1):
            bc = _bin_center(i)
            if bc < lo or bc > hi:
                continue
            d = bc - vw
            w = math.exp(-(d * d) / denom)
            weights.append(w)
            idxs.append(i)

        if idxs:
            wsum = sum(weights)
            if wsum > 0:
                for i, w in zip(idxs, weights):
                    bin_vols[i] += vg * (w / wsum)
            else:
                per = vg / len(idxs)
                for i in idxs:
                    bin_vols[i] += per
        else:
            bin_vols[_idx_from_price(cl)] += vg

        # ---------------------------
        # Body 30% (Hybrid B)
        # ---------------------------
        vb = 0.30 * v
        body_lo = min(o, cl)
        body_hi = max(o, cl)

        ba = _idx_from_price(body_lo)
        bb = _idx_from_price(body_hi)

        if ba > bb:
            ba, bb = bb, ba

        body_idxs = list(range(ba, bb + 1))

        if not body_idxs:
            bin_vols[_idx_from_price(cl)] += vb
        else:
            vb_u = 0.80 * vb
            vb_r = 0.20 * vb

            per_u = vb_u / len(body_idxs)
            for i in body_idxs:
                bin_vols[i] += per_u

            span = max(body_hi - body_lo, width)

            ramp_weights = []
            rsum = 0.0

            for i in body_idxs:
                bc = _bin_center(i)

                if cl >= o:
                    # bullish: ramp toward close upward
                    w = (bc - body_lo) / span
                else:
                    # bearish: ramp toward close downward
                    w = (body_hi - bc) / span

                if w < 0:
                    w = 0

                ramp_weights.append(w)
                rsum += w

            if rsum > 0:
                for i, w in zip(body_idxs, ramp_weights):
                    bin_vols[i] += vb_r * (w / rsum)
            else:
                bin_vols[_idx_from_price(cl)] += vb_r


    # POC (dual POC v1)
    # Rule: allow a 2nd POC only if its volume is within 10% of the top POC volume.
    POC2_TOL_RATIO = 0.10  # 10% tolerance => second qualifies if vol >= 90% of top

    if not bin_vols:
        poc_idx = 0
        poc2_idx = None
    else:
        # Sort indices by volume desc
        idx_sorted = sorted(range(bins_count), key=lambda i: bin_vols[i], reverse=True)
        poc_idx = idx_sorted[0]
        top_vol = float(bin_vols[poc_idx])

        poc2_idx = None
        if top_vol > 0:
            min_vol = top_vol * (1.0 - POC2_TOL_RATIO)
            MIN_SEP_BINS = 2  # locked spec

            for j in idx_sorted[1:]:
                if abs(j - poc_idx) < MIN_SEP_BINS:
                    continue  # too close to primary POC
            
                if float(bin_vols[j]) >= min_vol:
                    poc2_idx = j
                    break


    poc_price = p_min + (poc_idx + 0.5) * width
    poc2_price = (p_min + (poc2_idx + 0.5) * width) if poc2_idx is not None else None

    


    # Build bins (KEEP ALL bins to preserve contiguity for node detection)
    bins = []
    for i, vol in enumerate(bin_vols):
        bin_center = p_min + (i + 0.5) * width
        bins.append({"price": bin_center, "volume": float(vol)})


    return {
        "asof": candles[-1].get("ts"),
        "window": window,
        "poc": poc_price,           # POC1 (backward compatible)
        "poc2": poc2_price,         # POC2 (optional)
        "pocs": [p for p in [poc_price, poc2_price] if p is not None],  # convenience list
        "poc_meta": {
            "poc_algo": "dual_poc_v1",
            "poc2_tol_ratio": 0.10,
        },
        "bins": bins,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "bin_count": bins_count,
    }





def _vp_from_candles_fixed(
    candles_subset: List[Dict[str, Any]],
    *,
    bin_count: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Build a VP using the ENTIRE provided subset (fixed range).
    """
    if not candles_subset:
        return {"asof": None, "window": 0, "poc": None, "bins": [], "bin_count": bin_count or 0}
    return _compute_volume_profile(
        candles_subset,
        max_lookback=len(candles_subset),
        bin_count=bin_count,
    )



def _slice_by_ts(
    candles: List[Dict[str, Any]],
    start_ts: Optional[str],
    end_ts: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Slice candles inclusive using index positions when possible,
    falling back to inclusive ts string comparisons.

    Assumes candle['ts'] is ISO-like and comparable lexicographically.
    """
    if not candles or not start_ts or not end_ts:
        return []

    ts_to_idx: Dict[str, int] = {}
    for i, c in enumerate(candles):
        ts = c.get("ts")
        if isinstance(ts, str):
            ts_to_idx[ts] = i

    si = ts_to_idx.get(start_ts)
    ei = ts_to_idx.get(end_ts)

    if si is not None and ei is not None:
        if si <= ei:
            return candles[si : ei + 1]
        return candles[ei : si + 1]

    lo = min(start_ts, end_ts)
    hi = max(start_ts, end_ts)

    out: List[Dict[str, Any]] = []
    for c in candles:
        ts = c.get("ts")
        if isinstance(ts, str) and lo <= ts <= hi:
            out.append(c)
    return out


def _session_candles_rth(candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Return candles for the last candle's date_et within RTH:
      09:30:00 <= time_et <= 16:00:00
    Uses date_et/time_et already present on enriched candles.
    """
    if not candles:
        return []

    last_date = candles[-1].get("date_et")
    if not last_date:
        return []

    start_t = "09:30:00"
    end_t = "16:00:00"

    out: List[Dict[str, Any]] = []
    for c in candles:
        if c.get("date_et") != last_date:
            continue
        t = c.get("time_et")
        if isinstance(t, str) and start_t <= t <= end_t:
            out.append(c)
    return out


def _compute_session_vp(candles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Session VP (RTH-only) using the same session candle set as VWAP should use.
    """
    sess = _session_candles_rth(candles)
    if not sess:
        return None

    vp = _vp_from_candles_fixed(sess, bin_count=40)
    return {
        **vp,
        "kind": "session",
        "date_et": sess[-1].get("date_et"),
        "start_ts": sess[0].get("ts"),
        "end_ts": sess[-1].get("ts"),
    }


def _compute_daily_extremes_vp(candles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    VP between daily extremes (RTH day-low <-> day-high) for the current session date_et.
    Implemented as VP on candles between the timestamp of the day-low candle
    and the timestamp of the day-high candle (inclusive), regardless of which came first.
    """
    sess = _session_candles_rth(candles)
    if not sess:
        return None

    low_c = min(sess, key=lambda c: float(c.get("low", 0.0)))
    high_c = max(sess, key=lambda c: float(c.get("high", 0.0)))

    low_ts = low_c.get("ts")
    high_ts = high_c.get("ts")
    if not low_ts or not high_ts:
        return None

    span = _slice_by_ts(sess, low_ts, high_ts)
    if not span:
        return None

    vp = _vp_from_candles_fixed(span, bin_count=30)
    return {
        **vp,
        "kind": "daily_extremes",
        "date_et": sess[-1].get("date_et"),
        "day_low": float(low_c.get("low", 0.0)),
        "day_low_ts": low_ts,
        "day_high": float(high_c.get("high", 0.0)),
        "day_high_ts": high_ts,
        "start_ts": span[0].get("ts"),
        "end_ts": span[-1].get("ts"),
    }


def _compute_structural_vp(
    candles: List[Dict[str, Any]],
    structural: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Structural VP anchored at the last CONFIRMED structural point:
      last point with label in {"LL","HH"}.
    Range: anchor_ts -> now (developing).
    """
    if not candles:
        return None

    points = (structural or {}).get("points") or []

    anchor = None
    for p in reversed(points):
        lab = p.get("label")
        if lab in ("LL", "HH"):
            anchor = p
            break

    if not anchor:
        return None

    anchor_ts = anchor.get("ts")
    if not anchor_ts:
        return None

    end_ts = candles[-1].get("ts")
    span = _slice_by_ts(candles, anchor_ts, end_ts)
    if not span:
        return None


    vp = _vp_from_candles_fixed(span, bin_count=25)

    return {
        **vp,
        "kind": "structural",
        "mode": "developing",
        "anchor_label": anchor.get("label"),
        "anchor_ts": anchor_ts,
        "anchor_price": anchor.get("price"),
        "start_ts": span[0].get("ts"),
        "end_ts": span[-1].get("ts"),
    }



def _compute_daily_completed_swing_vp(
    candles: List[Dict[str, Any]],
    structural: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Daily Completed Swing VP:
      - Uses ONLY today's RTH session candles
      - Anchors to the most recent completed pair of confirmed extremes within the session:
          (HH then LL) or (LL then HH)
      - Requires structural.points labels to find confirmed extremes.
    """
    sess = _session_candles_rth(candles)
    if not sess:
        return None

    date_et = sess[-1].get("date_et")
    if not date_et:
        return None

    points = (structural or {}).get("points") or []
    if not points:
        return None

    # Map candle ts -> candle for session membership checks
    sess_ts_set = {c.get("ts") for c in sess if c.get("ts")}

    # Take confirmed extremes (HH/LL) that are inside today's session candles
    extremes = []
    for p in points:
        lab = p.get("label")
        ts = p.get("ts")
        if lab in ("HH", "LL") and ts in sess_ts_set:
            extremes.append(p)

    if len(extremes) < 2:
        return None

    # Find the most recent opposite-labeled pair (completed swing)
    end = extremes[-1]
    start = None
    for p in reversed(extremes[:-1]):
        if p.get("label") != end.get("label"):
            start = p
            break

    if not start:
        return None

    start_ts = start.get("ts")
    end_ts = end.get("ts")
    if not start_ts or not end_ts:
        return None

    span = _slice_by_ts(sess, start_ts, end_ts)
    if not span:
        return None

    vp = _vp_from_candles_fixed(span, bin_count=30)


    return {
        **vp,
        "kind": "completed_swing",
        "scope": "daily",
        "date_et": date_et,
        "direction": f"{start.get('label')}_to_{end.get('label')}",
        "start_label": start.get("label"),
        "end_label": end.get("label"),
        "start_ts": span[0].get("ts"),
        "end_ts": span[-1].get("ts"),
        "start_price": start.get("price"),
        "end_price": end.get("price"),
        "start_point_ts": start_ts,
        "end_point_ts": end_ts,
    }


def _compute_structural_completed_swing_vp(
    candles: List[Dict[str, Any]],
    structural: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Structural Completed Swing VP (multi-day capable):
      - Anchors to the most recent completed pair of confirmed extremes in structural.points:
          (HH then LL) or (LL then HH)
      - Uses full candle history (not session-limited)
      - Spans across days if the structure does.
    """
    if not candles:
        return None

    points = (structural or {}).get("points") or []
    if not points:
        return None

    extremes = [p for p in points if p.get("label") in ("HH", "LL") and p.get("ts")]
    if len(extremes) < 2:
        return None

    end = extremes[-1]
    start = None
    for p in reversed(extremes[:-1]):
        if p.get("label") != end.get("label"):
            start = p
            break

    if not start:
        return None

    start_ts = start.get("ts")
    end_ts = end.get("ts")
    if not start_ts or not end_ts:
        return None

    span = _slice_by_ts(candles, start_ts, end_ts)
    if not span:
        return None

    vp = _vp_from_candles_fixed(span, bin_count=25)

    return {
        **vp,
        "kind": "completed_swing",
        "scope": "structural",
        "direction": f"{start.get('label')}_to_{end.get('label')}",
        "start_label": start.get("label"),
        "end_label": end.get("label"),
        "start_ts": span[0].get("ts"),
        "end_ts": span[-1].get("ts"),
        "start_price": start.get("price"),
        "end_price": end.get("price"),
        "start_point_ts": start_ts,
        "end_point_ts": end_ts,
    }


def _vp_smooth_3(vols: List[float]) -> List[float]:
    if not vols:
        return []
    if len(vols) == 1:
        return vols[:]
    out = []
    for i in range(len(vols)):
        if i == 0:
            out.append((vols[0] + vols[1]) / 2.0)
        elif i == len(vols) - 1:
            out.append((vols[-2] + vols[-1]) / 2.0)
        else:
            out.append((vols[i - 1] + vols[i] + vols[i + 1]) / 3.0)
    return out


def _std(vals: List[float]) -> float:
    if not vals:
        return 0.0
    m = sum(vals) / len(vals)
    var = sum((x - m) ** 2 for x in vals) / len(vals)
    return math.sqrt(var)


def _percentile(vals: List[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    if p <= 0:
        return s[0]
    if p >= 100:
        return s[-1]
    k = (len(s) - 1) * (p / 100.0)
    f = int(math.floor(k))
    c = int(math.ceil(k))
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def _vp_median(vals: List[float]) -> float:
    return _percentile(vals, 50.0) if vals else 0.0


def _vp_mad(vals: List[float]) -> float:
    """Median absolute deviation (robust dispersion)."""
    if not vals:
        return 0.0
    m = _vp_median(vals)
    devs = [abs(x - m) for x in vals]
    return _vp_median(devs)


def _vp_is_local_max(v: List[float], i: int) -> bool:
    n = len(v)
    if n == 0:
        return False
    if i <= 0:
        return v[0] >= (v[1] if n > 1 else v[0])
    if i >= n - 1:
        return v[n - 1] >= v[n - 2]
    return v[i] >= v[i - 1] and v[i] > v[i + 1]


def _vp_is_local_min(v: List[float], i: int) -> bool:
    n = len(v)
    if n == 0:
        return False
    if i <= 0:
        return v[0] <= (v[1] if n > 1 else v[0])
    if i >= n - 1:
        return v[n - 1] <= v[n - 2]
    return v[i] <= v[i - 1] and v[i] < v[i + 1]


def _vp_expand_from_center(
    vs: List[float],
    active: List[bool],
    center: int,
    *,
    threshold: float,
    max_width_bins: int,
) -> Tuple[int, int]:
    """Expand left/right from center while vs >= threshold (and active)."""
    n = len(vs)
    a = b = center
    if not (0 <= center < n) or not active[center]:
        return center, center

    # expand left
    while a - 1 >= 0 and active[a - 1] and vs[a - 1] >= threshold:
        if (b - (a - 1) + 1) > max_width_bins:
            break
        a -= 1

    # expand right
    while b + 1 < n and active[b + 1] and vs[b + 1] >= threshold:
        if ((b + 1) - a + 1) > max_width_bins:
            break
        b += 1

    return a, b


def _vp_expand_valley_from_center(
    vs: List[float],
    active: List[bool],
    center: int,
    *,
    ceiling: float,
    max_width_bins: int,
) -> Tuple[int, int]:
    """Expand left/right from center while vs <= ceiling (and active)."""
    n = len(vs)
    a = b = center
    if not (0 <= center < n) or not active[center]:
        return center, center

    while a - 1 >= 0 and active[a - 1] and vs[a - 1] <= ceiling:
        if (b - (a - 1) + 1) > max_width_bins:
            break
        a -= 1

    while b + 1 < n and active[b + 1] and vs[b + 1] <= ceiling:
        if ((b + 1) - a + 1) > max_width_bins:
            break
        b += 1

    return a, b


def _vp_best_poc_idx(vols_raw: List[float], a: int, b: int) -> int:
    return max(range(a, b + 1), key=lambda k: vols_raw[k])


def _vp_build_nodes_from_mask(
    prices: List[float],
    vols_raw: List[float],
    mask: List[bool],
    *,
    kind: str,
    max_keep: int,
    noise_filter: bool = True,
) -> List[Dict[str, Any]]:
    """
    Cluster contiguous True regions in mask into range-nodes.
    kind: "hvn" or "lvn"
    """
    if not prices or not vols_raw or not mask:
        return []

    mean_vol = (sum(vols_raw) / len(vols_raw)) if vols_raw else 0.0

    clusters: List[Tuple[int, int]] = []
    i = 0
    n = len(mask)
    while i < n:
        if not mask[i]:
            i += 1
            continue
        j = i
        while j + 1 < n and mask[j + 1]:
            j += 1
        clusters.append((i, j))
        i = j + 1

    nodes: List[Dict[str, Any]] = []
    for (a, b) in clusters:
        seg_prices = prices[a : b + 1]
        seg_vols = vols_raw[a : b + 1]
        if not seg_prices or not seg_vols:
            continue

        vol_sum = float(sum(seg_vols))
        bin_cnt = int(len(seg_vols))

        # Noise filter: discard tiny clusters that are also low significance
        if noise_filter and bin_cnt < 2 and vol_sum < (1.2 * mean_vol):
            continue

        # POC inside cluster = max raw volume bin
        poc_i = max(range(a, b + 1), key=lambda k: vols_raw[k])
        poc_price = prices[poc_i]

        nodes.append(
            {
                "low": float(min(seg_prices)),
                "high": float(max(seg_prices)),
                "poc": float(poc_price),
                "vol_sum": float(vol_sum),
                "bin_count": bin_cnt,
            }
        )

    # Rank
    if kind == "hvn":
        nodes.sort(key=lambda x: x.get("vol_sum", 0.0), reverse=True)
    else:
        nodes.sort(key=lambda x: x.get("vol_sum", 0.0))

    nodes = nodes[: max_keep]

    # Add rank field
    for idx, nd in enumerate(nodes, start=1):
        nd["rank"] = idx

    return nodes


def _vp_add_nodes(vp: Optional[Dict[str, Any]], *, profile_name: str) -> Optional[Dict[str, Any]]:
    """
    Add hvn_ranges/lvn_ranges according to FINAL spec.

    This function builds *prop-desk style* HVN/LVN nodes from the raw VP bins:
      - HVNs are bounded peaks (volume bulges)
      - LVNs are bounded valleys (volume voids / low participation corridors)

    IMPORTANT:
      - This is the *geometry* layer only (price/volume).
      - Contextual "tradability" scoring (FVG/liquidity/trend confluence) is applied
        later by the bot (multi-TF ranker).
    """
    if not vp or not isinstance(vp, dict):
        return vp

    bins = vp.get("bins") or []
    if not isinstance(bins, list) or not bins:
        vp["hvn_ranges"] = []
        vp["lvn_ranges"] = []
        return vp

    # Ensure bins sorted by price
    bins_sorted = sorted(bins, key=lambda x: float(x.get("price", 0.0)))
    prices = [float(b.get("price", 0.0)) for b in bins_sorted]
    vols_raw = [float(b.get("volume", 0.0)) for b in bins_sorted]

    active = [v > 0.0 for v in vols_raw]
    if sum(1 for a in active if a) < 3:
        vp["hvn_ranges"] = []
        vp["lvn_ranges"] = []
        return vp

    # Profile parameters (tuned for your profiles)
    if profile_name in ("structural", "structural_completed_swing"):
        smooth = False
        max_h, max_l = 2, 2
        max_width_bins = 5
        drop_ratio = 0.70
        rise_ratio = 1.30
        peak_pctl = 80.0
        trough_pctl = 20.0
    elif profile_name == "rolling_300":
        smooth = True
        max_h, max_l = 5, 5
        max_width_bins = 10
        drop_ratio = 0.65
        rise_ratio = 1.40
        peak_pctl = 75.0
        trough_pctl = 25.0
    else:
        # rolling_60, session, daily_extremes, daily_completed_swing
        smooth = True
        max_h, max_l = 3, 3
        max_width_bins = 6
        drop_ratio = 0.60
        rise_ratio = 1.30
        peak_pctl = 75.0
        trough_pctl = 25.0

    vs = _vp_smooth_3(vols_raw) if smooth else vols_raw[:]
    vs_active = [vs[i] for i in range(len(vs)) if active[i]]
    if not vs_active:
        vp["hvn_ranges"] = []
        vp["lvn_ranges"] = []
        return vp

    med = _vp_median(vs_active)
    mad = _vp_mad(vs_active)
    mad_eps = max(mad, 1e-9)

    # Robust floors/ceilings with percentile backstop
    peak_floor = max(_percentile(vs_active, peak_pctl), med + 0.75 * mad)
    trough_ceiling = min(_percentile(vs_active, trough_pctl), max(0.0, med - 0.75 * mad))

    # Helper: width penalty (wider nodes are less "precise")
    def _width_penalty(bin_cnt: int) -> float:
        return 1.0 / math.sqrt(max(1, int(bin_cnt)))

    # Helper: stable base score scaling (keeps numbers reasonable but ordered)
    def _mass_term(vol_sum: float) -> float:
        return math.log1p(max(0.0, float(vol_sum)))

    # --- HVNs: bounded peaks ---
    peak_idx: List[int] = []
    for i in range(len(vs)):
        if not active[i]:
            continue
        if vs[i] < peak_floor:
            continue
        if _vp_is_local_max(vs, i):
            peak_idx.append(i)

    # Prefer strongest peaks first; avoid overlapping HVNs
    peak_idx.sort(key=lambda i: vs[i], reverse=True)
    hvn_nodes: List[Dict[str, Any]] = []
    occupied = [False] * len(vs)

    for i in peak_idx:
        thr = vs[i] * drop_ratio
        a, b = _vp_expand_from_center(vs, active, i, threshold=thr, max_width_bins=max_width_bins)

        # overlap check
        if any(occupied[k] for k in range(a, b + 1)):
            continue
        for k in range(a, b + 1):
            occupied[k] = True

        poc_i = _vp_best_poc_idx(vols_raw, a, b)
        vol_sum = float(sum(vols_raw[a : b + 1]))
        bin_cnt = int(b - a + 1)

        # "Professional" HVN geometry score:
        #  - mass (log1p vol_sum)
        #  - prominence vs robust center (z-like)
        #  - width penalty (prefer tighter HVNs)
        prominence = max(0.0, vs[i] - med)
        prom_z = prominence / mad_eps
        base_score = _mass_term(vol_sum) * (1.0 + prom_z) * _width_penalty(bin_cnt)

        hvn_nodes.append(
            {
                "low": float(prices[a]),
                "high": float(prices[b]),
                "poc": float(prices[poc_i]),
                "vol_sum": float(vol_sum),
                "bin_count": bin_cnt,
                "base_score": float(base_score),
                "_score": float(base_score),
                "_center": i,
            }
        )

    hvn_nodes.sort(key=lambda x: x.get("_score", 0.0), reverse=True)
    hvn_nodes = hvn_nodes[:max_h]

    # Re-sort by price for LVN valley construction
    hvn_nodes_by_price = sorted(hvn_nodes, key=lambda x: x.get("low", 0.0))

    # finalize ranks for HVN (base layer rank; may be overridden later)
    for r, nd in enumerate(hvn_nodes, start=1):
        nd.pop("_center", None)
        nd.pop("_score", None)
        nd["rank"] = r

    # --- LVNs: valleys between HVNs (preferred) ---
    lvn_nodes: List[Dict[str, Any]] = []
    if len(hvn_nodes_by_price) >= 2:
        def _idx_of_price(p: float) -> int:
            # prices are sorted; nearest index
            return min(range(len(prices)), key=lambda j: abs(prices[j] - p))

        for left, right in zip(hvn_nodes_by_price, hvn_nodes_by_price[1:]):
            li = _idx_of_price(float(left["high"]))
            ri = _idx_of_price(float(right["low"]))
            if ri - li < 2:
                continue

            # find trough in (li, ri)
            candidates = [j for j in range(li + 1, ri) if active[j]]
            if not candidates:
                continue

            t = min(candidates, key=lambda j: vs[j])
            if vs[t] > trough_ceiling:
                continue

            ceiling = vs[t] * rise_ratio
            a, b = _vp_expand_valley_from_center(vs, active, t, ceiling=ceiling, max_width_bins=max_width_bins)
            vol_sum = float(sum(vols_raw[a : b + 1]))
            bin_cnt = int(b - a + 1)

            # "Professional" LVN geometry score:
            #  - thinness (how empty vs median)
            #  - separation (more distance between HVNs -> more "travel corridor")
            #  - width penalty (prefer tighter LVNs)
            thinness = med / max(vs[t], 1.0)
            sep_bins = max(1, ri - li)
            sep_term = 1.0 + (sep_bins / max(1.0, float(len(prices))))  # bounded ~[1,2]
            base_score = thinness * sep_term * _width_penalty(bin_cnt)

            lvn_nodes.append(
                {
                    "low": float(prices[a]),
                    "high": float(prices[b]),
                    "poc": float(prices[t]),
                    "vol_sum": float(vol_sum),
                    "bin_count": bin_cnt,
                    "base_score": float(base_score),
                    "_score": float(base_score),
                }
            )

    # Fallback: local minima valleys if not enough HVNs
    if not lvn_nodes:
        trough_idx: List[int] = []
        for i in range(len(vs)):
            if not active[i]:
                continue
            if vs[i] > trough_ceiling:
                continue
            if _vp_is_local_min(vs, i):
                trough_idx.append(i)

        trough_idx.sort(key=lambda i: vs[i])
        occ = [False] * len(vs)

        for t in trough_idx:
            if any(occ[k] for k in range(max(0, t - 1), min(len(vs), t + 2))):
                continue

            ceiling = vs[t] * rise_ratio
            a, b = _vp_expand_valley_from_center(vs, active, t, ceiling=ceiling, max_width_bins=max_width_bins)
            for k in range(a, b + 1):
                occ[k] = True

            vol_sum = float(sum(vols_raw[a : b + 1]))
            bin_cnt = int(b - a + 1)

            thinness = med / max(vs[t], 1.0)
            # fallback has weaker separation info; assume moderate corridor
            sep_term = 1.25
            base_score = thinness * sep_term * _width_penalty(bin_cnt)

            lvn_nodes.append(
                {
                    "low": float(prices[a]),
                    "high": float(prices[b]),
                    "poc": float(prices[t]),
                    "vol_sum": float(vol_sum),
                    "bin_count": bin_cnt,
                    "base_score": float(base_score),
                    "_score": float(base_score),
                }
            )

    lvn_nodes.sort(key=lambda x: x.get("_score", 0.0), reverse=True)
    lvn_nodes = lvn_nodes[:max_l]

    for r, nd in enumerate(lvn_nodes, start=1):
        nd.pop("_score", None)
        nd["rank"] = r

    # Small meta for downstream rankers / debugging (safe additive change)
    vp.setdefault("meta", {})
    if isinstance(vp["meta"], dict):
        vp["meta"].setdefault("base_rank_basis", "base_score")
        vp["meta"].setdefault("node_algo", "bounded_peaks_valleys_v2")

    vp["hvn_ranges"] = hvn_nodes
    vp["lvn_ranges"] = lvn_nodes
    return vp


def compute_trend(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = [float(c["close"]) for c in candles]
    last = candles[-1]
    last_ts = last.get("ts")
    last_close = float(last["close"])

    ema50 = _ema(closes, 50)
    ema200 = _ema(closes, 200)

    dist50_abs = dist50_pct = None
    dist200_abs = dist200_pct = None
    if ema50 is not None:
        dist50_abs = last_close - ema50
        if ema50 != 0:
            dist50_pct = dist50_abs / ema50
    if ema200 is not None:
        dist200_abs = last_close - ema200
        if ema200 != 0:
            dist200_pct = dist200_abs / ema200

    # Simple state logic (v1)
    state: Optional[str] = None
    if ema50 is not None and ema200 is not None:
        if ema50 > ema200 and last_close > ema50:
            state = "bull"
        elif ema50 < ema200 and last_close < ema50:
            state = "bear"
        else:
            state = "range"
    else:
        state = "unknown"

    return {
        "asof": last_ts,
        "state": state,
        "ema50": ema50,
        "ema200": ema200,
        "dist_ema50_abs": dist50_abs,
        "dist_ema200_abs": dist200_abs,
        "dist_ema50_pct": dist50_pct,
        "dist_ema200_pct": dist200_pct,
    }



def compute_swings(candles: List[Dict[str, Any]], max_pivots: int = 20) -> Dict[str, Any]:
    """
    Backward-compatible wrapper.

    OLD meaning: candle-engine micro tags (HH/HL/LH/LL).
    NEW meaning: confirmed swings derived from confirmed pivots (len=1),
    strictly layered and non-repainting.

    NOTE: 'max_pivots' retained in signature for compatibility; now controls
    max swings returned (roughly).
    """
    piv = compute_pivots_len1(candles, max_pivots=max(60, max_pivots * 3))
    sw = compute_swings_from_pivots(piv, max_swings=max_pivots)
    return sw




def compute_pivots_len1(
    candles: List[Dict[str, Any]],
    max_pivots: int = 120,
) -> Dict[str, Any]:

    if len(candles) < 3:
        return {
            "asof": candles[-1].get("ts") if candles else None,
            "tf": (candles[-1].get("tf") or candles[-1].get("timeframe")) if candles else None,
            "pivot_len": 1,
            "lookback": len(candles),
            "max_pivots": max_pivots,
            "pivots": [],
        }

    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]

    pivots: List[Dict[str, Any]] = []

    for i in range(1, len(candles) - 1):
        ph = highs[i] > highs[i - 1] and highs[i] > highs[i + 1]
        pl = lows[i] < lows[i - 1] and lows[i] < lows[i + 1]

        if ph and pl:
            continue

        if not (ph or pl):
            continue

        pivot_type = "pivot_high" if ph else "pivot_low"
        pivot_price = float(candles[i]["high"] if ph else candles[i]["low"])

        # ---- context window (±4 candles) ----
        pre = candles[max(0, i - 4): i]
        post = candles[i + 1: i + 5]

        pre_vol = _avg([c.get("vol_rel") for c in pre if c.get("vol_rel") is not None])
        post_vol = _avg([c.get("vol_rel") for c in post if c.get("vol_rel") is not None])

        pre_mom = _avg([c.get("mom_atr") for c in pre if c.get("mom_atr") is not None])
        post_mom = _avg([c.get("mom_atr") for c in post if c.get("mom_atr") is not None])

        atr14 = _compute_atr(candles[: i + 1], period=14)

        excursion = None
        excursion_atr = None
        follow_through = False

        if atr14:
            if ph:
                post_extreme = max([float(c["high"]) for c in post], default=pivot_price)
                excursion = post_extreme - pivot_price
            else:
                post_extreme = min([float(c["low"]) for c in post], default=pivot_price)
                excursion = pivot_price - post_extreme

            excursion_atr = excursion / atr14 if atr14 else None
            follow_through = excursion_atr is not None and excursion_atr >= 0.5

        # ---- score ----
        follow_pts = 50.0 if follow_through else 0.0
        vol_pts = 25.0 * _clamp((post_vol or 0.0) / 2.0, 0.0, 1.0)
        mom_pts = 25.0 * _clamp(abs(post_mom or 0.0), 0.0, 1.0)

        score = round(follow_pts + vol_pts + mom_pts, 2)

        pivots.append(
            {
                "ts": candles[i].get("ts"),
                "tf": candles[i].get("tf") or candles[i].get("timeframe"),
                "type": pivot_type,
                "price": pivot_price,
                "candle": _mini_candle(candles[i]),
                "context": {
                    "pre": {"avg_vol_rel": pre_vol, "avg_mom_atr": pre_mom},
                    "post": {"avg_vol_rel": post_vol, "avg_mom_atr": post_mom},
                },
                "metrics": {
                    "atr14": atr14,
                    "excursion": excursion,
                    "excursion_atr": excursion_atr,
                    "follow_through": follow_through,
                },
                "score": score,
                "score_parts": {
                    "follow_pts": follow_pts,
                    "vol_pts": vol_pts,
                    "mom_pts": mom_pts,
                },
            }
        )

    if max_pivots and len(pivots) > max_pivots:
        pivots = pivots[-max_pivots:]

    return {
        "asof": candles[-1].get("ts"),
        "tf": candles[-1].get("tf") or candles[-1].get("timeframe"),
        "pivot_len": 1,
        "lookback": len(candles),
        "max_pivots": max_pivots,
        "pivots": pivots,
    }


def compute_swings_from_pivots(
    pivots_obj: Dict[str, Any],
    max_swings: int = 60,
) -> Dict[str, Any]:

    pivots = (pivots_obj or {}).get("pivots") or []
    reduced: List[Dict[str, Any]] = []

    for p in pivots:
        if not reduced:
            reduced.append(p)
            continue

        last = reduced[-1]
        if p["type"] == last["type"]:
            if p["type"] == "pivot_high" and p["price"] > last["price"]:
                reduced[-1] = p
            elif p["type"] == "pivot_low" and p["price"] < last["price"]:
                reduced[-1] = p
        else:
            reduced.append(p)

    confirmed = reduced[:-1] if len(reduced) >= 2 else []

    swings: List[Dict[str, Any]] = []
    for p in confirmed:
        swings.append(
            {
                "ts": p["ts"],
                "tf": p.get("tf") or (p.get("candle") or {}).get("tf"),
                "type": "swing_high" if p["type"] == "pivot_high" else "swing_low",
                "price": p["price"],
                "pivot": p,   # ← FULL inheritance
            }
        )

    if max_swings and len(swings) > max_swings:
        swings = swings[-max_swings:]


    return {
        "asof": pivots_obj.get("asof"),
        "tf": pivots_obj.get("tf"),
        "lookback_pivots": len(pivots),
        "max_swings": max_swings,
        "swings": swings,
    }





def compute_structural_from_swings(
    swings_obj: Dict[str, Any],
    max_points: int = 60,
) -> Dict[str, Any]:

    swings = (swings_obj or {}).get("swings") or []
    points: List[Dict[str, Any]] = []

    prev_high = None
    prev_low = None

    for s in swings:
        label = None
        if s["type"] == "swing_high":
            if prev_high is not None:
                label = "HH" if s["price"] > prev_high else "LH"
            prev_high = s["price"]
        else:
            if prev_low is not None:
                label = "HL" if s["price"] > prev_low else "LL"
            prev_low = s["price"]

        points.append(
            {
                "ts": s["ts"],
                "tf": s.get("tf") or (s.get("pivot") or {}).get("tf") or ((s.get("pivot") or {}).get("candle") or {}).get("tf"),
                "type": s["type"],
                "price": s["price"],
                "label": label,
                "swing": s,   # ← FULL inheritance
            }
        )

    if max_points and len(points) > max_points:
        points = points[-max_points:]

    # ------------------------------------------------------------
    # LOG ONLY: print newest labeled swing detected (live only)
    # ------------------------------------------------------------
    global _LAST_PRINTED_LIVE_POINT

    asof_ts = swings_obj.get("asof")
    tf = swings_obj.get("tf")

    if points and isinstance(asof_ts, str):
        newest = points[-1]

        point_ts = newest.get("ts")
        point_tf = newest.get("tf") or tf
        point_type = newest.get("type")
        point_price = newest.get("price")
        point_label = newest.get("label")

        if (
            isinstance(point_ts, str)
            and point_ts[:10] == asof_ts[:10]
            and point_type in ("swing_high", "swing_low")
            and point_price is not None
        ):
            point_key = (str(point_tf), point_ts, point_type, float(point_price), point_label)

            if _LAST_PRINTED_LIVE_POINT != point_key:
                # print(
                #     f"LIVE_SWING tf={point_tf}, ts={point_ts}, type={point_type}, "
                #     f"price={point_price}, label={point_label}"
                # )
                _LAST_PRINTED_LIVE_POINT = point_key

    return {
        "asof": swings_obj.get("asof"),
        "tf": swings_obj.get("tf"),
        "max_points": max_points,
        "points": points,
    }






def compute_fvgs(candles: List[Dict[str, Any]], max_lookback: int = 300) -> List[Dict[str, Any]]:
    """
    ICT-style 3-candle Fair Value Gap (FVG) detection, aligned with TradingView:

      Bullish FVG (up move):
        low(c3) > high(c1)
        FVG zone = [ high(c1), low(c3) ]

      Bearish FVG (down move):
        high(c3) < low(c1)
        FVG zone = [ high(c3), low(c1) ]

    We also track whether the gap has been filled by any subsequent candle.
    """
    if len(candles) < 3:
        return []

    fvgs: List[Dict[str, Any]] = []
    tail = candles[-max_lookback:]
    n = len(tail)

    for i in range(2, n):
        # 3-candle window
        c1 = tail[i - 2]
        c2 = tail[i - 1]  # not used in the condition, kept for clarity
        c3 = tail[i]

        h1 = float(c1["high"])
        l1 = float(c1["low"])
        h3 = float(c3["high"])
        l3 = float(c3["low"])

        direction: Optional[str] = None
        zone_low: float
        zone_high: float

        # Bullish FVG: low(c3) > high(c1)
        if l3 > h1:
            direction = "bull"
            zone_low = h1      # bottom of the gap
            zone_high = l3     # top of the gap

        # Bearish FVG: high(c3) < low(c1)
        elif h3 < l1:
            direction = "bear"
            zone_low = h3      # bottom of the gap
            zone_high = l1     # top of the gap

        else:
            continue

        created_ts = c3.get("ts")
        impulse_ts = c2.get("ts")

        asof_ts = tail[-1].get("ts")
        impulse_dt = _parse_ts(impulse_ts)
        asof_dt = _parse_ts(asof_ts)
        
        age_minutes = None
        if impulse_dt and asof_dt:
            age_minutes = (asof_dt - impulse_dt).total_seconds() / 60.0
        
        age_bars = (n - 1) - (i - 1)



        filled = False
        filled_ts = None
        
        touch_count = 0
        first_touch = None
        last_touch = None
        
        forward = tail[i + 1:]
        
        for c in forward:
            h = float(c["high"])
            l = float(c["low"])
            cl = float(c["close"])
        
            overlaps = not (h < zone_low or l > zone_high)
            if overlaps:
                touch_count += 1
                beh = _touch_behavior(c, zone_low, zone_high)
                if first_touch is None:
                    first_touch = beh
                last_touch = beh
        
            # CLOSE-BASED FILL ONLY
            if direction == "bull" and cl < zone_low:
                filled = True
                filled_ts = c["ts"]
                break
            if direction == "bear" and cl > zone_high:
                filled = True
                filled_ts = c["ts"]
                break



        filled_pct = 0.0
        if not filled:
            width = zone_high - zone_low
            if width > 0 and forward:   # ✅ guard: only compute if we actually have forward candles
                if direction == "bull":
                    min_low = min(float(c["low"]) for c in forward)
                    if min_low < zone_high:
                        filled_pct = min(100.0, (zone_high - min_low) / width * 100.0)
                else:
                    max_high = max(float(c["high"]) for c in forward)
                    if max_high > zone_low:
                        filled_pct = min(100.0, (max_high - zone_low) / width * 100.0)
            else:
                filled_pct = 0.0

        if filled:
            filled_pct = 100.0



        imp_idx = i - 1

        pre = tail[max(0, imp_idx - 3): imp_idx]
        post = tail[imp_idx + 1: imp_idx + 4]
        
        pre_bull = sum(1 for c in pre if _candle_dir(c) == "bull")
        pre_bear = sum(1 for c in pre if _candle_dir(c) == "bear")
        pre_net = _net_change_pct(pre)
        pre_dir_counts = _dir_by_counts(pre_bull, pre_bear)
        pre_dir_drift = _dir_by_drift(pre_net)
        
        post_bull = sum(1 for c in post if _candle_dir(c) == "bull")
        post_bear = sum(1 for c in post if _candle_dir(c) == "bear")
        post_net = _net_change_pct(post)
        post_dir_counts = _dir_by_counts(post_bull, post_bear)
        post_dir_drift = _dir_by_drift(post_net)
        
        score_status = "final" if len(pre) == 3 and len(post) == 3 else "not_enough_data"
        
        context = {
            "ref": "impulse",
            "pre_n": 3,
            "post_m": 3,
            "pre": {
                "bull_count": pre_bull,
                "bear_count": pre_bear,
                "net_change_pct": pre_net,
                "dir_by_counts": pre_dir_counts,
                "dir_by_drift": pre_dir_drift,
            },
            "post": {
                "bull_count": post_bull,
                "bear_count": post_bear,
                "net_change_pct": post_net,
                "dir_by_counts": post_dir_counts,
                "dir_by_drift": post_dir_drift,
            },
        }






        fvg_score = None
        style = None
        
        if score_status == "final":
            # impulse direction reference (bullish/bearish) based on impulse candle
            imp_ref_dir = "bullish" if _candle_dir(c2) == "bull" else "bearish"
        
            # alignment scores using BOTH drift + counts (finalized)
            pre_alignment = _alignment_score(imp_ref_dir, pre_dir_counts, pre_dir_drift)
            post_acceptance = _alignment_score(imp_ref_dir, post_dir_counts, post_dir_drift)
        
            # ATR for impulse normalization
            atr14 = _compute_atr(tail[:imp_idx + 1])
            body = float(c2.get("body") or abs(float(c2["close"]) - float(c2["open"])))
            body_vs_atr = (body / atr14) if atr14 else None
        
            # close strength + volume confirmation (finalized)
            spread_strength = c2.get("spread_strength", 0.0)
            try:
                spread_strength = float(spread_strength)
            except Exception:
                spread_strength = 0.0
            spread_strength = _clamp(spread_strength, 0.0, 1.0)
        
            vol_rel = c2.get("vol_rel", None)
            try:
                vol_rel = float(vol_rel) if vol_rel is not None else None
            except Exception:
                vol_rel = None
        
            n_body = _norm_body_vs_atr(body_vs_atr)
            n_spread = spread_strength
            n_vol = _norm_vol_rel(vol_rel)
        
            # FINAL fvg_score weights:
            # impulse 70 = body/ATR(40) + close_strength(15) + vol_rel(15)
            impulse_points = (40.0 * n_body) + (15.0 * n_spread) + (15.0 * n_vol)
        
            # context 30 = post_acceptance(20) + pre_structure(10)
            context_points = (20.0 * post_acceptance) + (10.0 * pre_alignment)
        
            fvg_score = float(_clamp(impulse_points + context_points, 0.0, 100.0))
        
            # impulse_quality_norm for style (normalize using same impulse weights / 70)
            imp_q_norm = _clamp((impulse_points / 70.0) if 70.0 > 0 else 0.0, 0.0, 1.0)
        
            # FINAL style scores (static): 40/40/20 using BOTH drift+counts
            cont = 100.0 * (0.40 * pre_alignment + 0.40 * post_acceptance + 0.20 * imp_q_norm)
            rev = 100.0 * (0.40 * (1.0 - pre_alignment) + 0.40 * post_acceptance + 0.20 * imp_q_norm)
        
            diff = abs(cont - rev)
            if cont >= rev + 15.0:
                label = "continuation"
            elif rev >= cont + 15.0:
                label = "reversal"
            else:
                label = "neutral"
        
            confidence = float(_clamp(diff / 50.0 * 100.0, 0.0, 100.0))
        
            style = {
                "continuation_score": float(_clamp(cont, 0.0, 100.0)),
                "reversal_score": float(_clamp(rev, 0.0, 100.0)),
                "label": label,
                "confidence": confidence,
            }


        
        trade_score = _trade_score_final(filled, filled_pct, touch_count, last_touch)

        fvgs.append(
            {
                "direction": direction,
                "created_ts": created_ts,
                "impulse_ts": impulse_ts,
                "high": zone_high,
                "low": zone_low,
        
                "filled": filled,
                "filled_ts": filled_ts,
                "filled_pct": filled_pct,
        
                "touch_count": touch_count,
                "first_touch_ts": first_touch["ts"] if first_touch else None,
                "first_touch": first_touch,
                "last_touch": last_touch,
        
                "asof_ts": asof_ts,
                "age_bars": age_bars,
                "age_minutes": age_minutes,
        
                "score_status": score_status,
                "fvg_score": fvg_score,
                "trade_score": trade_score,
                "style": style,
                "context": context,
                
            }
        )


    return fvgs




def compute_liquidity(
    candles: List[Dict[str, Any]],
    swings: Dict[str, Any],
    max_levels: int = 120,
    eq_atr_mult: float = 0.08,
    eq_min_abs: float = 0.05,
) -> Dict[str, Any]:
    """
    Derive liquidity information from confirmed swings + candles.

    Changes in v2:
      - Equal highs/lows are CLUSTERS (not pairs)
      - Wick sweeps / close breaks are no longer returned as separate event arrays
        Instead, per-level counters + candle_ts lists are stored directly on:
          * swing levels (inside liquidity.levels)
          * equal clusters (liquidity.equal_highs / liquidity.equal_lows)
      - first/last candle_ts are stored for O(1) recency checks
    """
    if not candles:
        return {"asof": None, "levels": [], "equal_highs": [], "equal_lows": [], "clean_highs": [], "clean_lows": []}

    last_ts = candles[-1].get("ts")
    pivots = swings.get("swings") or []

    # ---------------------------
    # 1) Base swing levels (latest N)
    # ---------------------------
    levels: List[Dict[str, Any]] = []
    for p in reversed(pivots):
        typ = p.get("type")
        if typ not in ("swing_high", "swing_low"):
            continue
        pivot = (p.get("pivot") or {}) if isinstance(p.get("pivot"), dict) else {}
        metrics = pivot.get("metrics") if isinstance(pivot.get("metrics"), dict) else {}
        atr14 = metrics.get("atr14")
        try:
            atr14 = float(atr14) if atr14 is not None else None
        except Exception:
            atr14 = None

        lvl = {
            "type": typ,
            "price": float(p.get("price") or 0.0),
            "ts": p.get("ts"),
            "atr14": atr14,
        }
        _init_level_counters(lvl)
        levels.append(lvl)
        if len(levels) >= max_levels:
            break
    levels.reverse()

    # ---------------------------
    # 2) Equal clusters (ATR-based tolerance, non-drifting)
    # ---------------------------
    equal_highs = _detect_equal_clusters(
        pivots=pivots,
        side="high",
        atr_mult=eq_atr_mult,
        min_abs=eq_min_abs,
        max_swings=max_levels,
    )
    equal_lows = _detect_equal_clusters(
        pivots=pivots,
        side="low",
        atr_mult=eq_atr_mult,
        min_abs=eq_min_abs,
        max_swings=max_levels,
    )

    # ---------------------------
    # 3) Clean highs / lows (unchanged list outputs)
    # ---------------------------
    clean_highs, clean_lows = _detect_clean_levels(
        candles=candles,
        pivots=pivots,
    )
    # Attach counters to clean levels too (handy for debugging/scoring)
    for x in (clean_highs or []):
        _init_level_counters(x)
        x["type"] = "clean_high"
    for x in (clean_lows or []):
        _init_level_counters(x)
        x["type"] = "clean_low"

    # ---------------------------
    # 4) Populate counters by scanning recent closed candles
    # ---------------------------
    # Keep the scan window bounded (prevents unbounded JSON growth).
    # This DOES NOT change your "keep all" rule within this liquidity window.
    recent_pivots = pivots[-max_levels:] if pivots else []
    scan_n = max(300, len(recent_pivots) * 2)
    scan_candles = candles[-scan_n:] if len(candles) > scan_n else candles

    # Build lookup for clean levels by ts
    clean_high_ts = {x.get("ts") for x in (clean_highs or []) if x.get("ts")}
    clean_low_ts = {x.get("ts") for x in (clean_lows or []) if x.get("ts")}

    # Map swing ts -> level dict for quick updates
    swing_by_ts: Dict[str, Dict[str, Any]] = {}
    for lvl in levels:
        ts = lvl.get("ts")
        if isinstance(ts, str):
            swing_by_ts[ts] = lvl

    # For each candle, evaluate against swing levels + equal clusters + clean levels
    # We detect "break events" as: close beyond level AND previous candle was NOT close beyond level
    last_beyond: Dict[Tuple[str, str, float], bool] = {}

    def _candle_type(c: Dict[str, Any]) -> str:
        try:
            o = float(c.get("open", 0.0))
            cl = float(c.get("close", 0.0))
        except Exception:
            return "doji"
        if cl > o:
            return "bull"
        if cl < o:
            return "bear"
        return "doji"

    for idx, c in enumerate(scan_candles):
        c_ts = c.get("ts")
        if not isinstance(c_ts, str):
            continue

        nxt = scan_candles[idx + 1] if (idx + 1) < len(scan_candles) else None
        try:
            high = float(c.get("high", 0.0))
            low = float(c.get("low", 0.0))
            close = float(c.get("close", 0.0))
        except Exception:
            continue

        next_close = None
        if isinstance(nxt, dict):
            try:
                next_close = float(nxt.get("close", 0.0))
            except Exception:
                next_close = None

        # ---- Swing levels ----
        for lvl in levels:
            l_ts = lvl.get("ts")
            if not isinstance(l_ts, str) or c_ts <= l_ts:
                continue
            price = float(lvl.get("price") or 0.0)
            if price <= 0:
                continue

            key = ("swing", l_ts, float(price))

            if lvl["type"] == "swing_high":
                beyond = close > price
                if beyond:
                    prev = last_beyond.get(key, False)
                    is_event = not prev
                    nh = (next_close is not None and next_close > price)
                    _record_break(
                        lvl,
                        c_ts,
                        is_event=is_event,
                        candle_type=_candle_type(c),
                        next_hold=nh,
                    )
                elif high > price and close < price:
                    _record_wick(lvl, c_ts)

                last_beyond[key] = beyond

            else:  # swing_low
                beyond = close < price
                if beyond:
                    prev = last_beyond.get(key, False)
                    is_event = not prev
                    nh = (next_close is not None and next_close < price)
                    _record_break(
                        lvl,
                        c_ts,
                        is_event=is_event,
                        candle_type=_candle_type(c),
                        next_hold=nh,
                    )
                elif low < price and close > price:
                    _record_wick(lvl, c_ts)

                last_beyond[key] = beyond

        # ---- Equal clusters ----
        for eq in (equal_highs or []):
            formed_ts = eq.get("formed_ts")
            if not isinstance(formed_ts, str) or c_ts <= formed_ts:
                continue
            price = float(eq.get("price") or 0.0)
            if price <= 0:
                continue

            key = ("eqH", formed_ts, float(price))

            beyond = close > price
            if beyond:
                prev = last_beyond.get(key, False)
                is_event = not prev
                nh = (next_close is not None and next_close > price)
                _record_break(
                    eq,
                    c_ts,
                    is_event=is_event,
                    candle_type=_candle_type(c),
                    next_hold=nh,
                )
            elif high > price and close < price:
                _record_wick(eq, c_ts)

            last_beyond[key] = beyond

        for eq in (equal_lows or []):
            formed_ts = eq.get("formed_ts")
            if not isinstance(formed_ts, str) or c_ts <= formed_ts:
                continue
            price = float(eq.get("price") or 0.0)
            if price <= 0:
                continue

            key = ("eqL", formed_ts, float(price))

            beyond = close < price
            if beyond:
                prev = last_beyond.get(key, False)
                is_event = not prev
                nh = (next_close is not None and next_close < price)
                _record_break(
                    eq,
                    c_ts,
                    is_event=is_event,
                    candle_type=_candle_type(c),
                    next_hold=nh,
                )
            elif low < price and close > price:
                _record_wick(eq, c_ts)

            last_beyond[key] = beyond

        # ---- Clean levels ----
        for cl in (clean_highs or []):
            l_ts = cl.get("ts")
            if not isinstance(l_ts, str) or c_ts <= l_ts:
                continue
            price = float(cl.get("price") or 0.0)
            if price <= 0:
                continue

            key = ("cleanH", l_ts, float(price))

            beyond = close > price
            if beyond:
                prev = last_beyond.get(key, False)
                is_event = not prev
                nh = (next_close is not None and next_close > price)
                _record_break(
                    cl,
                    c_ts,
                    is_event=is_event,
                    candle_type=_candle_type(c),
                    next_hold=nh,
                )
            elif high > price and close < price:
                _record_wick(cl, c_ts)

            last_beyond[key] = beyond

        for cl in (clean_lows or []):
            l_ts = cl.get("ts")
            if not isinstance(l_ts, str) or c_ts <= l_ts:
                continue
            price = float(cl.get("price") or 0.0)
            if price <= 0:
                continue

            key = ("cleanL", l_ts, float(price))

            beyond = close < price
            if beyond:
                prev = last_beyond.get(key, False)
                is_event = not prev
                nh = (next_close is not None and next_close < price)
                _record_break(
                    cl,
                    c_ts,
                    is_event=is_event,
                    candle_type=_candle_type(c),
                    next_hold=nh,
                )
            elif low < price and close > price:
                _record_wick(cl, c_ts)

            last_beyond[key] = beyond


    # State: mark broken when break_count > 0
    for obj in (levels or []) + (equal_highs or []) + (equal_lows or []) + (clean_highs or []) + (clean_lows or []):
        if int(obj.get("break_count") or 0) > 0:
            obj["state"] = "broken"
            obj["broken_ts"] = obj.get("first_break_ts")
        else:
            obj["state"] = "active"
            obj["broken_ts"] = None

        # convenience: last_event_ts
        obj["last_event_ts"] = obj.get("last_break_ts") or obj.get("last_wick_ts")

    return {
        "asof": last_ts,
        "levels": levels,
        "equal_highs": equal_highs,
        "equal_lows": equal_lows,
        "clean_highs": clean_highs,
        "clean_lows": clean_lows,
    }


def _init_level_counters(obj: Dict[str, Any]) -> None:
    # Wick interactions
    obj.setdefault("wick_count", 0)
    obj.setdefault("wick_candles_ts", [])
    obj.setdefault("first_wick_ts", None)
    obj.setdefault("last_wick_ts", None)

    # Break interactions (close beyond)
    # break_count / break_candles_ts = ONLY "break events" (first candle of a break run)
    obj.setdefault("break_count", 0)
    obj.setdefault("break_candles_ts", [])
    obj.setdefault("first_break_ts", None)
    obj.setdefault("last_break_ts", None)

    # New:
    # break_close_count = counts ALL candles that close beyond the level (even after first event)
    obj.setdefault("break_close_count", 0)

    # New:
    # break_events = list of the event candles only, with extra metadata
    # Each item: {"ts": ..., "candle_type": "bull"|"bear"|"doji", "next_hold": bool}
    obj.setdefault("break_events", [])



def _record_wick(obj: Dict[str, Any], candle_ts: str) -> None:
    _init_level_counters(obj)
    arr = obj.get("wick_candles_ts") or []
    # Keep unique + ordered (append-only)
    if not arr or arr[-1] != candle_ts:
        if candle_ts not in set(arr):
            arr.append(candle_ts)
            obj["wick_candles_ts"] = arr
            obj["wick_count"] = int(obj.get("wick_count") or 0) + 1
            if obj.get("first_wick_ts") is None:
                obj["first_wick_ts"] = candle_ts
            obj["last_wick_ts"] = candle_ts


def _record_break(
    obj: Dict[str, Any],
    candle_ts: str,
    *,
    is_event: bool,
    candle_type: Optional[str] = None,
    next_hold: Optional[bool] = None,
) -> None:
    """
    Break tracking:
      - Always increment break_close_count when called (caller calls it for ANY close beyond).
      - If is_event=True, also record the event candle in break_candles_ts + break_events and increment break_count.

    This lets you keep:
      - All "true break event" candle TS (start of each break run)
      - Only a COUNT for all other closes beyond the level
    """
    _init_level_counters(obj)

    # Count every close beyond the level (even if it's the 100th candle beyond)
    obj["break_close_count"] = int(obj.get("break_close_count") or 0) + 1

    if not is_event:
        return

    # Maintain append-only unique list of *event* break candle TS
    arr = obj.get("break_candles_ts") or []
    if candle_ts not in set(arr):
        arr.append(candle_ts)
        obj["break_candles_ts"] = arr

        obj["break_count"] = int(obj.get("break_count") or 0) + 1
        if obj.get("first_break_ts") is None:
            obj["first_break_ts"] = candle_ts
        obj["last_break_ts"] = candle_ts

        ev = obj.get("break_events") or []
        ev.append(
            {
                "ts": candle_ts,
                "candle_type": candle_type,
                "next_hold": next_hold,
            }
        )
        obj["break_events"] = ev






def _detect_equal_clusters(
    pivots: List[Dict[str, Any]],
    side: str,
    atr_mult: float = 0.05,
    min_abs: float = 0.02,
    max_swings: int = 120,
) -> List[Dict[str, Any]]:
    """
    Detect equal highs/lows as CLUSTERS (not pairs).

    - Works on confirmed swings in `pivots` (which are swings, not raw pivots).
    - Uses ATR snapshot at swing time: swing["pivot"]["metrics"]["atr14"] when available.
    - Non-drifting: tolerance is computed at the time of comparing a swing into a cluster,
      using the stricter ATR (min of swing_atr and cluster reference_atr).
    """
    out: List[Dict[str, Any]] = []
    if not pivots:
        return out

    want_type = "swing_high" if side == "high" else "swing_low"

    # Work on a bounded recent set (prevents combinatorial blowup)
    recent = [p for p in pivots[-max_swings:] if p.get("type") == want_type and p.get("ts") and p.get("price")]

    clusters: List[Dict[str, Any]] = []
    for s in recent:
        s_ts = s.get("ts")
        try:
            s_price = float(s.get("price") or 0.0)
        except Exception:
            continue
        if s_price <= 0 or not isinstance(s_ts, str):
            continue

        pivot = (s.get("pivot") or {}) if isinstance(s.get("pivot"), dict) else {}
        metrics = pivot.get("metrics") if isinstance(pivot.get("metrics"), dict) else {}
        atr14 = metrics.get("atr14")
        try:
            s_atr = float(atr14) if atr14 is not None else None
        except Exception:
            s_atr = None

        placed = False
        for cl in clusters:
            c_price = float(cl.get("price") or 0.0)
            ref_atr = cl.get("ref_atr")
            try:
                ref_atr = float(ref_atr) if ref_atr is not None else None
            except Exception:
                ref_atr = None

            eff_atr = None
            if s_atr is not None and ref_atr is not None:
                eff_atr = min(s_atr, ref_atr)
            else:
                eff_atr = s_atr or ref_atr

            tol = max(float(min_abs), float(atr_mult) * float(eff_atr or 0.0))
            if tol <= 0:
                tol = float(min_abs)

            if abs(s_price - c_price) <= tol:
                # add to cluster
                cl["touches_ts"].append(s_ts)
                cl["touch_prices"].append(s_price)
                # reference ATR = min of member ATRs (stricter)
                if s_atr is not None:
                    if cl.get("ref_atr") is None:
                        cl["ref_atr"] = s_atr
                    else:
                        cl["ref_atr"] = min(float(cl["ref_atr"]), s_atr)
                # recompute cluster price as median
                cl["price"] = float(_median(cl["touch_prices"]))
                # formed_ts = latest touch (cluster exists after the last swing that defines it)
                cl["formed_ts"] = max(cl["touches_ts"])
                placed = True
                break

        if not placed:
            cl = {
                "kind": "equal",
                "side": side,
                "touches_ts": [s_ts],
                "touch_prices": [s_price],
                "price": s_price,
                "ref_atr": s_atr,
                "formed_ts": s_ts,
            }
            _init_level_counters(cl)
            clusters.append(cl)

    # Keep only clusters with 2+ touches
    for cl in clusters:
        if len(cl.get("touches_ts") or []) >= 2:
            # trim internal helper list for output cleanliness
            cl.pop("touch_prices", None)
            out.append(cl)

    return out


def _median(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    try:
        return float(__import__("statistics").median([float(x) for x in vals]))
    except Exception:
        return None




def _detect_clean_levels(
    candles: List[Dict[str, Any]],
    pivots: List[Dict[str, Any]],
    look_ahead_bars: int = 100,
    max_levels: int = 120,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    clean_highs: List[Dict[str, Any]] = []
    clean_lows: List[Dict[str, Any]] = []

    if not candles or not pivots:
        return clean_highs, clean_lows

    ts_to_idx: Dict[str, int] = {}
    for idx, c in enumerate(candles):
        ts = c.get("ts")
        if isinstance(ts, str):
            ts_to_idx[ts] = idx

    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]

    for p in reversed(pivots):
        if len(clean_highs) >= max_levels and len(clean_lows) >= max_levels:
            break

        typ = p.get("type")
        price = float(p.get("price") or 0.0)
        ts = p.get("ts")
        if typ not in ("swing_high", "swing_low") or not ts or price == 0:
            continue

        idx = ts_to_idx.get(ts)
        if idx is None:
            continue

        start = idx + 1
        end = min(len(candles), start + look_ahead_bars)

        violated = False
        if typ == "swing_high":
            for k in range(start, end):
                if highs[k] > price:
                    violated = True
                    break
            if not violated:
                clean_highs.append({"price": price, "ts": ts})
        else:  # swing_low
            for k in range(start, end):
                if lows[k] < price:
                    violated = True
                    break
            if not violated:
                clean_lows.append({"price": price, "ts": ts})

    clean_highs.reverse()
    clean_lows.reverse()
    return clean_highs, clean_lows



def _detect_wick_sweeps(
    candles: List[Dict[str, Any]],
    pivots: List[Dict[str, Any]],
    lookback_bars: int = 50,
) -> List[Dict[str, Any]]:
    """
    Detect wick sweeps of recent swing highs/lows:
      - sweep_high: candle high > prior swing high AND candle close < that swing high
      - sweep_low : candle low  < prior swing low  AND candle close > that swing low

    IMPORTANT PATCHES:
      1) Enforce candle_ts > swing_ts (prevents “self-sweep” where ts are equal)
      2) Record ALL sweeps (no early break after first match per candle)
    """
    sweeps: List[Dict[str, Any]] = []
    if not candles or not pivots:
        return sweeps

    recent_pivots = pivots[-lookback_bars:]

    swing_highs = [
        (float(p.get("price") or 0.0), p.get("ts"))
        for p in recent_pivots
        if p.get("type") == "swing_high"
    ]
    swing_lows = [
        (float(p.get("price") or 0.0), p.get("ts"))
        for p in recent_pivots
        if p.get("type") == "swing_low"
    ]

    for c in candles[-lookback_bars:]:
        ts = c.get("ts")
        if not ts:
            continue

        high = float(c["high"])
        low = float(c["low"])
        close = float(c["close"])

        # Sweep highs (record ALL)
        for sh_price, sh_ts in swing_highs:
            if not sh_ts or not sh_price:
                continue

            # Enforce: candle must be AFTER the swing
            # ISO strings with same timezone sort correctly lexicographically
            if isinstance(ts, str) and isinstance(sh_ts, str) and ts <= sh_ts:
                continue

            if high > sh_price and close < sh_price:
                sweeps.append(
                    {
                        "type": "sweep_high",
                        "swept_price": sh_price,
                        "swing_ts": sh_ts,
                        "candle_ts": ts,
                    }
                )

        # Sweep lows (record ALL)
        for sl_price, sl_ts in swing_lows:
            if not sl_ts or not sl_price:
                continue

            if isinstance(ts, str) and isinstance(sl_ts, str) and ts <= sl_ts:
                continue

            if low < sl_price and close > sl_price:
                sweeps.append(
                    {
                        "type": "sweep_low",
                        "swept_price": sl_price,
                        "swing_ts": sl_ts,
                        "candle_ts": ts,
                    }
                )

    return sweeps


    
def _detect_close_breaks(
    candles: List[Dict[str, Any]],
    pivots: List[Dict[str, Any]],
    lookback_pivots: int = 120,
) -> List[Dict[str, Any]]:
    """
    Detect close-based breaks:
      - side=up   : close > prior swing_high price
      - side=down : close < prior swing_low price

    IMPORTANT PATCH:
      Enforce candle_ts > swing_ts (a candle can’t “break” a swing that is the same candle)
    """
    breaks: List[Dict[str, Any]] = []
    if not candles or not pivots:
        return breaks

    recent_pivots = pivots[-lookback_pivots:]
    if not recent_pivots:
        return breaks

    swing_highs = [
        (float(p.get("price") or 0.0), p.get("ts"))
        for p in recent_pivots
        if p.get("type") == "swing_high"
    ]
    swing_lows = [
        (float(p.get("price") or 0.0), p.get("ts"))
        for p in recent_pivots
        if p.get("type") == "swing_low"
    ]

    recent_candles = candles[-max(50, len(recent_pivots) * 2):]

    for c in recent_candles:
        ts = c.get("ts")
        if not ts:
            continue

        close = float(c["close"])

        # Break highs (record ALL)
        for sh_price, sh_ts in swing_highs:
            if not sh_ts or not sh_price:
                continue
            if isinstance(ts, str) and isinstance(sh_ts, str) and ts <= sh_ts:
                continue
            if close > sh_price:
                breaks.append(
                    {
                        "side": "up",
                        "broken_price": sh_price,
                        "swing_ts": sh_ts,
                        "candle_ts": ts,
                    }
                )

        # Break lows (record ALL)
        for sl_price, sl_ts in swing_lows:
            if not sl_ts or not sl_price:
                continue
            if isinstance(ts, str) and isinstance(sl_ts, str) and ts <= sl_ts:
                continue
            if close < sl_price:
                breaks.append(
                    {
                        "side": "down",
                        "broken_price": sl_price,
                        "swing_ts": sl_ts,
                        "candle_ts": ts,
                    }
                )

    return breaks




def compute_extras(candles: List[Dict[str, Any]], vol_lookback: int = 50) -> Dict[str, Any]:
    """
    Miscellaneous helpers (ATR, vol regime, etc.).
    """
    last = candles[-1]
    last_ts = last.get("ts")

    atr_val = _compute_atr(candles)

    # Very rough vol regime using vol_rel, if present
    vol_rels: List[float] = []
    for c in candles[-vol_lookback:]:
        vr = c.get("vol_rel")
        try:
            if vr is not None:
                vol_rels.append(float(vr))
        except Exception:
            continue

    vol_regime: Optional[str] = None
    if vol_rels:
        avg_vr = sum(vol_rels) / len(vol_rels)
        if avg_vr >= 2.0:
            vol_regime = "high"
        elif avg_vr <= 0.5:
            vol_regime = "low"
        else:
            vol_regime = "normal"
    else:
        vol_regime = "unknown"

    return {
        "asof": last_ts,
        "atr": atr_val,
        "vol_regime": vol_regime,
    }



def compute_structure_state(
    trend: Dict[str, Any],
    structural: Dict[str, Any],
    liquidity: Dict[str, Any],
) -> Optional[str]:
    """
    Multi-factor structure state used by the strategy layer.

    Design goals:
    - Prefer event-time ordering over string ordering when possible.
    - Separate continuation (BOS with trend) from reversal (CHoCH-style mismatch).
    - Use recent swing labels to confirm directional intent.
    """

    def _latest_ts(ts_values: List[Any]) -> Optional[dt.datetime]:
        parsed = [_parse_ts(v) for v in ts_values if isinstance(v, str)]
        parsed = [p for p in parsed if p is not None]
        if not parsed:
            return None
        return max(parsed)

    trend_state_raw = str(trend.get("state") or "unknown").strip().lower()
    trend_state_alias = {
        "bull": "bullish",
        "bullish": "bullish",
        "bear": "bearish",
        "bearish": "bearish",
        "range": "range",
        "neutral": "range",
        "unknown": "unknown",
    }
    trend_state = trend_state_alias.get(trend_state_raw, trend_state_raw or "unknown")

    points = structural.get("points") or []
    recent_points = points[-6:] if points else []
    recent_labels = {p.get("label") for p in recent_points}

    bullish_score = 0
    bearish_score = 0
    for lbl in [p.get("label") for p in recent_points]:
        if lbl in {"HH", "HL"}:
            bullish_score += 1
        elif lbl in {"LH", "LL"}:
            bearish_score += 1

    structure_bias = "neutral"
    if bullish_score > bearish_score:
        structure_bias = "bullish"
    elif bearish_score > bullish_score:
        structure_bias = "bearish"

    # BOS side derived from the most recent broken swing level (close-based break)
    bos_side: Optional[str] = None
    last_up_ts: Optional[dt.datetime] = None
    last_down_ts: Optional[dt.datetime] = None

    for lvl in (liquidity or {}).get("levels") or []:
        ltype = lvl.get("type")
        ts = _latest_ts([lvl.get("last_break_ts"), lvl.get("broken_ts"), lvl.get("last_event_ts")])
        if ts is None:
            continue
        if ltype == "swing_high":
            if last_up_ts is None or ts > last_up_ts:
                last_up_ts = ts
        elif ltype == "swing_low":
            if last_down_ts is None or ts > last_down_ts:
                last_down_ts = ts

    if last_up_ts and (not last_down_ts or last_up_ts > last_down_ts):
        bos_side = "up"
    elif last_down_ts and (not last_up_ts or last_down_ts > last_up_ts):
        bos_side = "down"

    trend_dir = trend_state if trend_state in {"bullish", "bearish"} else "neutral"
    bos_dir = "bullish" if bos_side == "up" else "bearish" if bos_side == "down" else "neutral"

    # Continuation vs CHoCH/reversal framing (closer to discretionary trader language)
    if trend_dir in {"bullish", "bearish"} and bos_dir == trend_dir and structure_bias == trend_dir:
        return f"{trend_state}_continuation_bos"

    if trend_dir in {"bullish", "bearish"} and bos_dir in {"bullish", "bearish"} and bos_dir != trend_dir:
        if structure_bias == bos_dir:
            return f"{trend_state}_choch_confirmed"
        return f"{trend_state}_choch_early"

    if trend_dir in {"bullish", "bearish"} and structure_bias == trend_dir and bos_dir == "neutral":
        return f"{trend_state}_trend_intact"

    if structure_bias in {"bullish", "bearish"} and trend_dir == "neutral":
        return f"{structure_bias}_structure_building"

    if recent_labels and {"HH", "HL", "LH", "LL"}.issuperset(recent_labels):
        return "range_or_transition"

    return trend_state



def compute_all_indicators(
    candles: List[Dict[str, Any]]
) -> Dict[str, Any]:
    trend = compute_trend(candles)

    pivots = compute_pivots_len1(candles)
    swings = compute_swings_from_pivots(pivots)
    structural = compute_structural_from_swings(swings)

    fvgs = compute_fvgs(candles)
    liquidity = compute_liquidity(candles, swings)

    # Propagate liquidity counters onto swings as well (so the swings column is self-contained)
    try:
        lvl_by_ts = {l.get("ts"): l for l in (liquidity.get("levels") or []) if isinstance(l.get("ts"), str)}
        for s in (swings.get("swings") or []):
            ts = s.get("ts")
            if not isinstance(ts, str):
                continue
            lvl = lvl_by_ts.get(ts)
            if not lvl:
                continue
            for k in (
                "wick_count","wick_candles_ts","first_wick_ts","last_wick_ts",
                "break_count","break_candles_ts","first_break_ts","last_break_ts",
                "state","broken_ts","last_event_ts",
            ):
                if k in lvl:
                    s[k] = lvl.get(k)
    except Exception:
        pass


    # --- Volume Profiles (fixed bins) ---
    vp_rolling_300 = _compute_volume_profile(candles, max_lookback=300, bin_count=60)
    vp_rolling_300 = _vp_add_nodes(vp_rolling_300, profile_name="rolling_300")
    
    vp_rolling_60 = None
    if len(candles) >= 60:
        vp_rolling_60 = _compute_volume_profile(candles, max_lookback=60, bin_count=40)
        # make it explicit it's rolling window 60
        vp_rolling_60["kind"] = "rolling"
        vp_rolling_60["window"] = 60
        vp_rolling_60 = _vp_add_nodes(vp_rolling_60, profile_name="rolling_60")
    
    vp_session = _compute_session_vp(candles)
    if vp_session:
        vp_session["bin_count"] = 40
        vp_session = _vp_add_nodes(vp_session, profile_name="session")
    
    vp_daily_extremes = _compute_daily_extremes_vp(candles)
    if vp_daily_extremes:
        vp_daily_extremes["bin_count"] = 30
        vp_daily_extremes = _vp_add_nodes(vp_daily_extremes, profile_name="daily_extremes")
    
    vp_structural = _compute_structural_vp(candles, structural)
    if vp_structural:
        vp_structural["bin_count"] = 25
        vp_structural = _vp_add_nodes(vp_structural, profile_name="structural")
    
    vp_daily_completed_swing = _compute_daily_completed_swing_vp(candles, structural)
    if vp_daily_completed_swing:
        vp_daily_completed_swing["bin_count"] = 30
        vp_daily_completed_swing = _vp_add_nodes(vp_daily_completed_swing, profile_name="daily_completed_swing")
    
    vp_structural_completed_swing = _compute_structural_completed_swing_vp(candles, structural)
    if vp_structural_completed_swing:
        vp_structural_completed_swing["bin_count"] = 25
        vp_structural_completed_swing = _vp_add_nodes(vp_structural_completed_swing, profile_name="structural_completed_swing")


    volume_profile = {
        **vp_rolling_300,  # keep top-level rolling_300 alias (backward compatible)
    
        "profiles": {
            "rolling_300": vp_rolling_300,
            "rolling_60": vp_rolling_60 if vp_rolling_60 else None,
            "session": vp_session,
            "daily_extremes": vp_daily_extremes,
            "structural": vp_structural,
            "daily_completed_swing": vp_daily_completed_swing,
            "structural_completed_swing": vp_structural_completed_swing,
        },
    }
    extras = compute_extras(candles)

    structure_state = compute_structure_state(trend, structural, liquidity)

    return {
        "trend": trend,
        "pivots": pivots,
        "swings": swings,
        "structural": structural,
        "fvgs": fvgs,
        "liquidity": liquidity,
        "volume_profile": volume_profile,
        "extras": extras,
        "structure_state": structure_state,
    }
