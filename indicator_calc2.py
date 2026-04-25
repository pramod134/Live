# indicator_calc2.py
#
# Phase 2 indicator calculations.
#
# This module is PURE: no DB, no CandleEngine, no network.
# It takes:
#   - candles: full history for this (symbol, timeframe)
#   - base_snapshot: output from indicator_calc1.compute_all_indicators(...)
#   - optional htf_snapshot: a higher-timeframe snapshot (same shape as base_snapshot)
#
# and returns a dict to be stored in `spot_tf.extras_advanced`:
#
# {
#   "momentum": { ... },
#   "vwap": { ... },
#   "htf": { ... },
#   "liq_summary": { ... },
#   "vol_context": { ... },
# }
#
# All values are as-of the LAST candle in `candles`.

from typing import Any, Dict, List, Optional
import math

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _ema(series: List[float], length: int) -> Optional[float]:
    """
    Simple EMA for the entire series; returns the LAST EMA value.
    If not enough data, returns None.
    """
    if length <= 0 or len(series) < length:
        return None

    k = 2.0 / (length + 1.0)
    ema = series[0]
    for v in series[1:]:
        ema = (v * k) + (ema * (1.0 - k))
    return ema


def _compute_rsi(closes: List[float], length: int = 14) -> Optional[float]:
    """
    Standard RSI (Wilder) on closes; returns last RSI value.
    If not enough data, returns None.
    """
    if len(closes) < length + 1:
        return None

    gains: List[float] = []
    losses: List[float] = []

    for i in range(1, length + 1):
        change = closes[i] - closes[i - 1]
        if change >= 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(-change)

    avg_gain = sum(gains) / length
    avg_loss = sum(losses) / length

    if len(closes) > length + 1:
        for i in range(length + 1, len(closes)):
            change = closes[i] - closes[i - 1]
            gain = max(change, 0.0)
            loss = max(-change, 0.0)

            avg_gain = (avg_gain * (length - 1) + gain) / length
            avg_loss = (avg_loss * (length - 1) + loss) / length

    if avg_loss == 0:
        if avg_gain == 0:
            return 50.0
        return 100.0

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi


def _compute_macd(
    closes: List[float],
    fast_length: int = 12,
    slow_length: int = 26,
    signal_length: int = 9,
) -> Dict[str, Optional[float]]:
    """
    Classic MACD (fast EMA - slow EMA) with signal EMA and histogram.
    Returns a dict of single values (last value of each).
    If not enough data, values may be None.
    """
    min_len = slow_length + signal_length
    if len(closes) < min_len:
        # Not enough history for a meaningful MACD
        return {
            "macd_fast": None,
            "macd_slow": None,
            "macd_line": None,
            "macd_signal": None,
            "macd_hist": None,
            "macd_hist_slope_3": None,
        }

    fast_series: List[Optional[float]] = []
    slow_series: List[Optional[float]] = []
    macd_series: List[float] = []

    for i in range(len(closes)):
        sub = closes[: i + 1]
        fast = _ema(sub, fast_length)
        slow = _ema(sub, slow_length)
        fast_series.append(fast)
        slow_series.append(slow)

        if fast is not None and slow is not None:
            macd_series.append(fast - slow)

    if not macd_series or len(macd_series) < signal_length:
        return {
            "macd_fast": fast_series[-1],
            "macd_slow": slow_series[-1],
            "macd_line": None,
            "macd_signal": None,
            "macd_hist": None,
            "macd_hist_slope_3": None,
        }

    macd_line = macd_series[-1]
    signal = _ema(macd_series, signal_length)
    hist = macd_line - signal if signal is not None else None

    macd_hist_slope_3 = None
    if hist is not None and len(macd_series) >= signal_length + 3:
        prev_signal = _ema(macd_series[:-3], signal_length)
        if prev_signal is not None:
            prev_hist = macd_series[-4] - prev_signal
            macd_hist_slope_3 = hist - prev_hist

    return {
        "macd_fast": fast_series[-1],
        "macd_slow": slow_series[-1],
        "macd_line": macd_line,
        "macd_signal": signal,
        "macd_hist": hist,
        "macd_hist_slope_3": macd_hist_slope_3,
    }


def _extract_price_from_liq_obj(obj: Any) -> Optional[float]:
    """
    Liquidity objects can be:
    - a float/int (price directly)
    - a dict with keys like 'price', 'level', 'value', 'high', 'low'
    This helper tries a few common patterns and returns a float or None.
    """
    if isinstance(obj, (int, float)):
        return float(obj)

    if isinstance(obj, dict):
        for key in ("price", "level", "value", "high", "low"):
            if key in obj:
                val = _safe_float(obj.get(key))
                if val is not None:
                    return val

    return None


# ---------------------------------------------------------------------------
# Block builders
# ---------------------------------------------------------------------------

def _build_momentum_block(
    candles: List[Dict[str, Any]],
    base_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    closes: List[float] = []
    for c in candles:
        close = _safe_float(c.get("close"))
        if close is not None:
            closes.append(close)

    if not closes:
        return {}

    last_close = closes[-1]
    atr = _safe_float(
        (base_snapshot.get("extras") or {}).get("atr")
    )

    # --- RSI value + behavior ---
    rsi_series: List[Optional[float]] = []
    for i in range(len(closes)):
        rsi_series.append(_compute_rsi(closes[: i + 1], 14))

    rsi_14 = rsi_series[-1]
    rsi_prev_3 = rsi_series[-4] if len(rsi_series) >= 4 else None

    rsi_slope_3: Optional[float] = None
    if rsi_14 is not None and rsi_prev_3 is not None:
        rsi_slope_3 = rsi_14 - rsi_prev_3

    rsi_regime: Optional[str] = None
    if rsi_14 is not None:
        if rsi_14 > 60:
            rsi_regime = "bullish"
        elif rsi_14 < 40:
            rsi_regime = "bearish"
        else:
            rsi_regime = "neutral"

    rsi_shift: Optional[str] = None
    if rsi_prev_3 is not None and rsi_14 is not None:
        if rsi_prev_3 < 50 <= rsi_14:
            rsi_shift = "bullish_shift"
        elif rsi_prev_3 > 50 >= rsi_14:
            rsi_shift = "bearish_shift"

    mom_raw: Optional[float] = None
    mom_norm: Optional[float] = None
    lookback = 5
    if len(closes) > lookback:
        mom_raw = closes[-1] - closes[-1 - lookback]
        if atr and atr > 0:
            mom_norm = mom_raw / atr

    # --- Momentum behavior ---
    mom_prev_3_norm: Optional[float] = None
    mom_slope_3: Optional[float] = None

    if len(closes) > lookback + 3 and atr and atr > 0:
        prev_raw = closes[-4] - closes[-4 - lookback]
        mom_prev_3_norm = prev_raw / atr

    if mom_norm is not None and mom_prev_3_norm is not None:
        mom_slope_3 = mom_norm - mom_prev_3_norm

    mom_regime: Optional[str] = None
    if mom_norm is not None:
        if mom_norm > 0.25:
            mom_regime = "bullish"
        elif mom_norm < -0.25:
            mom_regime = "bearish"
        else:
            mom_regime = "neutral"

    mom_strength: Optional[str] = None
    if mom_norm is not None:
        abs_m = abs(mom_norm)
        if abs_m < 0.25:
            mom_strength = "weak"
        elif abs_m < 0.75:
            mom_strength = "normal"
        elif abs_m < 1.5:
            mom_strength = "strong"
        else:
            mom_strength = "extreme"

    mom_exhaustion = bool(mom_norm is not None and abs(mom_norm) > 1.5)

    macd_vals = _compute_macd(closes)
    macd_fast = macd_vals.get("macd_fast")
    macd_slow = macd_vals.get("macd_slow")
    macd_line = macd_vals.get("macd_line")
    macd_signal = macd_vals.get("macd_signal")
    macd_hist = macd_vals.get("macd_hist")
    macd_hist_slope_3 = macd_vals.get("macd_hist_slope_3")

    macd_hist_norm: Optional[float] = None
    if macd_hist is not None and atr and atr > 0:
        macd_hist_norm = macd_hist / atr

    return {
        "rsi_14": rsi_14,
        "rsi_prev_3": rsi_prev_3,
        "rsi_slope_3": rsi_slope_3,
        "rsi_regime": rsi_regime,
        "rsi_shift": rsi_shift,
        "mom_raw": mom_raw,
        "mom_norm": mom_norm,
        "mom_prev_3_norm": mom_prev_3_norm,
        "mom_slope_3": mom_slope_3,
        "mom_regime": mom_regime,
        "mom_strength": mom_strength,
        "mom_exhaustion": mom_exhaustion,
        "macd_fast": macd_fast,
        "macd_slow": macd_slow,
        "macd_line": macd_line,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist,
        "macd_hist_slope_3": macd_hist_slope_3,
        "macd_hist_norm": macd_hist_norm,
        "atr_for_norm": atr,
        "last_close": last_close,
    }


def _build_vwap_block(
    candles: List[Dict[str, Any]],
    base_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Compute a simple RTH VWAP using all candles provided.
    If you later restrict candles to "today only" at the caller level,
    this automatically becomes "today's VWAP".
    """
    total_pv = 0.0
    total_vol = 0.0

    for c in candles:
        high = _safe_float(c.get("high"))
        low = _safe_float(c.get("low"))
        close = _safe_float(c.get("close"))
        vol = _safe_float(c.get("volume")) or 0.0

        if high is None or low is None or close is None:
            continue

        typical_price = (high + low + close) / 3.0
        total_pv += typical_price * vol
        total_vol += vol

    if total_vol <= 0:
        vwap = None
    else:
        vwap = total_pv / total_vol

    # distance & extension flag
    last_close = None
    if candles:
        last_close = _safe_float(candles[-1].get("close"))

    dist_from_vwap_pct: Optional[float] = None
    extended_flag: Optional[str] = None

    if vwap is not None and last_close is not None and vwap != 0:
        dist_from_vwap_pct = (last_close - vwap) / vwap

        # Simple extension thresholds; you can tune these later.
        upper_th = 0.015  # +1.5%
        lower_th = -0.015  # -1.5%

        if dist_from_vwap_pct > upper_th:
            extended_flag = "extended_above"
        elif dist_from_vwap_pct < lower_th:
            extended_flag = "extended_below"
        else:
            extended_flag = "normal"

    return {
        "vwap_rth": vwap,
        "dist_from_vwap_pct": dist_from_vwap_pct,
        "extended_flag": extended_flag,
    }


def _build_htf_block(
    htf_snapshot: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Multi-timeframe bias summary, if the caller passes an HTF snapshot
    (same shape as base_snapshot, e.g. from 60m).
    """
    if not htf_snapshot:
        return {}

    trend_state = htf_snapshot.get("structure_state")
    extras = htf_snapshot.get("extras") or {}

    # Very simple bias from structure_state; can be refined later.
    bias: Optional[str] = None
    if isinstance(trend_state, str):
        ts = trend_state.lower()
        if "bull" in ts:
            bias = "bull"
        elif "bear" in ts:
            bias = "bear"
        else:
            bias = "range"

    return {
        "trend_state": trend_state,
        "bias": bias,
        # If we want to surface HTF RSI later:
        # "rsi_14": (extras.get("rsi_14") if isinstance(extras, dict) else None),
    }


def _build_liq_summary_block(
    candles: List[Dict[str, Any]],
    base_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Summarize liquidity from calc1's 'liquidity' object into a few distances.
    This is intentionally defensive: it handles floats OR dicts with price-like keys.
    """
    liquidity = base_snapshot.get("liquidity") or {}
    clean_highs = liquidity.get("clean_highs") or []
    clean_lows = liquidity.get("clean_lows") or []
    eq_highs = liquidity.get("equal_highs") or []
    eq_lows = liquidity.get("equal_lows") or []

    last_close = None
    if candles:
        last_close = _safe_float(candles[-1].get("close"))

    nearest_clean_high_price: Optional[float] = None
    nearest_clean_low_price: Optional[float] = None
    nearest_clean_high_dist_pct: Optional[float] = None
    nearest_clean_low_dist_pct: Optional[float] = None

    if last_close is not None:
        above_dists: List[tuple[float, float]] = []
        below_dists: List[tuple[float, float]] = []

        for item in clean_highs:
            price = _extract_price_from_liq_obj(item)
            if price is None:
                continue
            if price >= last_close:
                dist_pct = (price - last_close) / last_close
                above_dists.append((dist_pct, price))

        for item in clean_lows:
            price = _extract_price_from_liq_obj(item)
            if price is None:
                continue
            if price <= last_close:
                dist_pct = (price - last_close) / last_close
                below_dists.append((dist_pct, price))

        if above_dists:
            above_dists.sort(key=lambda x: abs(x[0]))
            nearest_clean_high_dist_pct, nearest_clean_high_price = above_dists[0]

        if below_dists:
            below_dists.sort(key=lambda x: abs(x[0]))
            nearest_clean_low_dist_pct, nearest_clean_low_price = below_dists[0]

    eq_high_stack_count = len(eq_highs) if isinstance(eq_highs, list) else 0
    eq_low_stack_count = len(eq_lows) if isinstance(eq_lows, list) else 0

    return {
        "nearest_clean_high_price": nearest_clean_high_price,
        "nearest_clean_high_dist_pct": nearest_clean_high_dist_pct,
        "nearest_clean_low_price": nearest_clean_low_price,
        "nearest_clean_low_dist_pct": nearest_clean_low_dist_pct,
        "eq_high_stack_count": eq_high_stack_count,
        "eq_low_stack_count": eq_low_stack_count,
    }


def _build_vol_context_block(
    candles: List[Dict[str, Any]],
    base_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Simple volatility context using current ATR and last bar range.
    More advanced stats (ADR, ATR percentile) can be added later.
    """
    if not candles:
        return {}

    last = candles[-1]
    high = _safe_float(last.get("high"))
    low = _safe_float(last.get("low"))
    atr = _safe_float((base_snapshot.get("extras") or {}).get("atr"))

    bar_range_vs_atr: Optional[float] = None
    if high is not None and low is not None and atr and atr > 0:
        bar_range = high - low
        bar_range_vs_atr = bar_range / atr

    return {
        "bar_range_vs_atr": bar_range_vs_atr,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_advanced_extras(
    candles: List[Dict[str, Any]],
    base_snapshot: Dict[str, Any],
    htf_snapshot: Optional[Dict[str, Any]] = None,
    session_candles: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Compute phase-2 indicators for the LAST candle, using:
      - `candles`: intraday or higher TF candles for this symbol/TF
      - `base_snapshot`: result from indicator_calc1.compute_all_indicators(candles)
      - `htf_snapshot`: OPTIONAL higher-timeframe snapshot (same shape as base_snapshot)

    Returns a dict intended to be stored directly into `spot_tf.extras_advanced`:

    {
      "momentum": { ... },
      "vwap": { ... },
      "htf": { ... },
      "liq_summary": { ... },
      "vol_context": { ... },
    }
    """
    if not candles:
        return {}

    momentum = _build_momentum_block(candles, base_snapshot)

    # VWAP should be based on session-only candles if provided.
    vwap_input = session_candles if session_candles is not None else candles
    vwap = _build_vwap_block(vwap_input, base_snapshot)

    htf = _build_htf_block(htf_snapshot)
    liq_summary = _build_liq_summary_block(candles, base_snapshot)
    vol_context = _build_vol_context_block(candles, base_snapshot)

    return {
        "momentum": momentum,
        "vwap": vwap,
        "htf": htf,
        "liq_summary": liq_summary,
        "vol_context": vol_context,
    }
