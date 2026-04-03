# bot/market_data.py

import os
import datetime as dt
from typing import Any, Dict, List, Optional

import httpx
from zoneinfo import ZoneInfo

# Timezone for RTH filtering
_EASTERN = ZoneInfo("America/New_York")

# Per-timeframe config: generic key -> (Alpaca timeframe, limit)
_TIMEFRAME_CONFIG: Dict[str, tuple[str, int]] = {
    "1m": ("1Min", 5000),
    "3m": ("3Min", 2500),
    "5m": ("5Min", 1500),
    "15m": ("15Min", 600),
    "1h": ("1Hour", 400),
    "1d": ("1Day", 200),
    "1w": ("1Week", 100),
}


# Number of days of history to request for each timeframe (A1 configuration)
_START_DATE_BY_TF = {
    "1Min": 30,       # 30 days of 1m candles
    "3Min": 30,       # 30 days of 3m candles
    "5Min": 30,       # 30 days of 5m candles
    "15Min": 30,      # 30 days of 15m candles
    "1Hour": 60,      # 60 days of 1-hour candles
    "1Day": 365,      # 1 year of daily candles
    "1Week": 1095     # ~3 years of weekly candles
}


def _get_alpaca_base_url() -> str:
    """
    Resolve Alpaca data base URL from env, with a sensible default.
    """
    return os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required for Alpaca market data")
    return value


def build_alpaca_headers() -> Dict[str, str]:
    """
    Build headers for Alpaca data API using standard env vars.

    Preferred names:
      - APCA_API_KEY_ID
      - APCA_API_SECRET_KEY

    Fallbacks:
      - ALPACA_API_KEY
      - ALPACA_API_SECRET
    """
    api_key = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY")
    api_secret = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_API_SECRET")

    if not api_key or not api_secret:
        raise RuntimeError(
            "Missing Alpaca API credentials. "
            "Set APCA_API_KEY_ID / APCA_API_SECRET_KEY "
            "or ALPACA_API_KEY / ALPACA_API_SECRET."
        )

    return {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
    }


def _parse_alpaca_ts(ts_str: str) -> dt.datetime:
    """
    Parse Alpaca ISO8601 timestamp '2025-11-26T21:05:00Z' into an aware UTC datetime.
    """
    # Accept both 'Z' and '+00:00' endings
    if ts_str.endswith("Z"):
        ts_str = ts_str.replace("Z", "+00:00")
    return dt.datetime.fromisoformat(ts_str)


def _is_rth_bar(ts_utc: dt.datetime) -> bool:
    """
    Return True if the bar's *start time* falls inside US regular trading hours
    (09:30–16:00 America/New_York) on a weekday.

    For intraday bars, this means:
      09:30 <= start < 16:00
    so the last valid bar starts at 15:59 (1m), 15:45 (15m), etc.
    """
    ts_et = ts_utc.astimezone(_EASTERN)
    # Monday=0, Sunday=6; require weekday
    if ts_et.weekday() > 4:
        return False

    t = ts_et.time()

    # Disallow anything before 09:30
    if t.hour < 9 or (t.hour == 9 and t.minute < 30):
        return False

    # Disallow anything starting at or after 16:00
    if t.hour >= 16:
        return False

    return True


def _normalize_bar(bar: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert an Alpaca bar object into our internal candle format.
    Alpaca bar fields:
      - t: timestamp (ISO string)
      - o: open
      - h: high
      - l: low
      - c: close
      - v: volume
    """
    ts_str = str(bar.get("t"))
    ts_dt = _parse_alpaca_ts(ts_str)

    return {
        "ts": ts_dt.isoformat(),
        "open": float(bar.get("o", 0.0)),
        "high": float(bar.get("h", 0.0)),
        "low": float(bar.get("l", 0.0)),
        "close": float(bar.get("c", 0.0)),
        "volume": float(bar.get("v", 0.0)),
    }


def _estimate_trading_days_needed(timeframe: str, limit: int) -> int:
    """
    Estimate how many TRADING days we need to go back to cover `limit` bars
    for the given timeframe.

    We treat trading days as weekdays (Mon–Fri). This overestimates slightly
    around holidays but that's fine; we just need start <= earliest candle time.
    """
    # RTH bars per day
    if timeframe == "1Min":
        bars_per_day = 390  # 6.5h * 60
    elif timeframe == "3Min":
        bars_per_day = 130  # 390 / 3
    elif timeframe == "5Min":
        bars_per_day = 78   # 390 / 5
    elif timeframe == "15Min":
        bars_per_day = 26   # 390 / 15
    elif timeframe == "1Hour":
        bars_per_day = 7    # ~6.5h ≈ 7 hourly bars
    elif timeframe == "1Day":
        bars_per_day = 1
    elif timeframe == "1Week":
        # 1 bar per week => 1 bar per 5 trading days
        bars_per_day = 1 / 5.0
    else:
        # Fallback: assume 1 bar per trading day
        bars_per_day = 1

    if bars_per_day <= 0:
        bars_per_day = 1

    trading_days = int((limit / bars_per_day) + 2)  # +2 as safety margin
    if trading_days < 5:
        trading_days = 5
    return trading_days


def _compute_start_date_for_timeframe(timeframe: str, limit: int) -> str:
    """
    Compute a start DATE string (YYYY-MM-DD) that is guaranteed to be earlier
    than or equal to the earliest candle we care about, counting only trading
    days as weekdays (Mon–Fri).
    """
    trading_days_needed = _estimate_trading_days_needed(timeframe, limit)

    # Start from "today" (UTC date), then walk backwards, counting only weekdays.
    current = dt.datetime.now(dt.timezone.utc).date()
    counted = 0

    # Walk back until we've hit the desired number of trading days.
    # This guarantees start <= earliest candle time we want, even if there are holidays.
    while counted < trading_days_needed:
        current -= dt.timedelta(days=1)
        if current.weekday() < 5:  # Mon–Fri
            counted += 1

    return current.isoformat()  # 'YYYY-MM-DD'

async def _fetch_alpaca_bars(
    client: httpx.AsyncClient,
    symbols: List[str],
    timeframe: str,
    limit: int,
    rth_only: bool = True,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Low-level helper to fetch bars from Alpaca for one timeframe, with full pagination.

    Uses the multi-symbol endpoint:
      /v2/stocks/bars?symbols=...&timeframe=...&limit=...&start=...&end=...&feed=...

    We request a date window (start/end) big enough for `limit` bars, then:
      - follow `next_page_token` until it is null
      - combine all pages
      - RTH-filter intraday frames
      - sort and clip to the last `limit` bars per symbol
    """
    if not symbols:
        return {}

    base_url = _get_alpaca_base_url()
    url = f"{base_url}/v2/stocks/bars"

    # Ensure no duplicates & normalized upper-case
    symbols_norm = sorted({s.strip().upper() for s in symbols if s.strip()})
    if not symbols_norm:
        return {}

    # Use "now" as end of the window
    end_dt = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    end_str = end_dt.isoformat().replace("+00:00", "Z")

    # Determine start date using config (A1 style)
    days_back = _START_DATE_BY_TF.get(timeframe, 30)
    start_dt = end_dt - dt.timedelta(days=days_back)
    start_str = start_dt.isoformat().replace("+00:00", "Z")

    # Base params for all pages
    base_params = {
        "symbols": ",".join(symbols_norm),
        "timeframe": timeframe,
        "start": start_str,
        "end": end_str,
        "limit": str(limit),
        "feed": os.getenv("ALPACA_DATA_FEED", "iex"),
        "adjustment": "raw",
    }

    # Merge headers: caller may have some; we ensure Alpaca auth is there.
    headers = dict(client.headers)
    headers.update(build_alpaca_headers())

    # Accumulator for all pages
    result: Dict[str, List[Dict[str, Any]]] = {sym: [] for sym in symbols_norm}

    page_token: Optional[str] = None

    while True:
        params = dict(base_params)
        if page_token:
            params["page_token"] = page_token

        resp = await client.get(url, params=params, headers=headers, timeout=30.0)

        # Debug logging (trim later if noisy)
        text_preview = resp.text[:500]

        resp.raise_for_status()
        data = resp.json()

        raw_bars = data.get("bars") or {}

        # Shape 1: {"bars": {"SPY": [...], "QQQ": [...], ...}}
        if isinstance(raw_bars, dict):
            for sym, bars in raw_bars.items():
                sym_u = sym.upper()
                if sym_u not in result:
                    continue

                for bar in bars:
                    ts_str = str(bar.get("t"))
                    ts_dt = _parse_alpaca_ts(ts_str)

                    # RTH filter only for intraday timeframes
                    if rth_only and timeframe in {"1Min", "3Min", "5Min", "15Min", "1Hour"}:
                        if not _is_rth_bar(ts_dt):
                            continue

                    result[sym_u].append(
                        {
                            "ts": ts_dt.isoformat(),
                            "open": float(bar.get("o", 0.0)),
                            "high": float(bar.get("h", 0.0)),
                            "low": float(bar.get("l", 0.0)),
                            "close": float(bar.get("c", 0.0)),
                            "volume": float(bar.get("v", 0.0)),
                        }
                    )

        # Shape 2: {"bars": [{"S": "SPY", ...}, ...]}
        elif isinstance(raw_bars, list):
            for bar in raw_bars:
                sym_u = str(bar.get("S", "")).upper()
                if sym_u not in result:
                    continue

                ts_dt = _parse_alpaca_ts(str(bar.get("t")))
                if rth_only and timeframe in {"1Min", "3Min", "5Min", "15Min", "1Hour"}:
                    if not _is_rth_bar(ts_dt):
                        continue

                result[sym_u].append(
                    {
                        "ts": ts_dt.isoformat(),
                        "open": float(bar.get("o", 0.0)),
                        "high": float(bar.get("h", 0.0)),
                        "low": float(bar.get("l", 0.0)),
                        "close": float(bar.get("c", 0.0)),
                        "volume": float(bar.get("v", 0.0)),
                    }
                )
        else:
            break

        # Get next_page_token; if null/empty, we're done
        page_token = data.get("next_page_token")
        if not page_token:
            break

    # Final sort + clip per symbol
    for sym, candles in result.items():
        candles.sort(key=lambda c: c["ts"])
        if len(candles) > limit:
            result[sym] = candles[-limit:]

    return result




async def fetch_multi_tf_candles(
    client: httpx.AsyncClient,
    symbols: List[str],
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    High-level function: fetch candles for multiple timeframes from Alpaca.

    For each symbol, returns RTH-only candles for:
      - 1m:  5000 bars
      - 3m:  2500 bars
      - 5m:  1500 bars
      - 15m: 600 bars
      - 1h:  400 bars
      - 1d:  200 bars
      - 1w:  100 bars

    Args:
        client: httpx.AsyncClient configured for outbound HTTP
        symbols: list of symbols (case-insensitive)

    Returns:
        {
          "SPY": {
            "1m":  [...],
            "3m":  [...],
            "5m":  [...],
            "15m": [...],
            "1h":  [...],
            "1d":  [...],
            "1w":  [...],
          },
          "AMD": { ... },
          ...
        }
    """
    symbols_norm = sorted({s.strip().upper() for s in symbols if s.strip()})
    if not symbols_norm:
        return {}

    out: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
        sym: {tf: [] for tf in _TIMEFRAME_CONFIG.keys()} for sym in symbols_norm
    }

    # We fetch each timeframe in turn.
    # If you want to optimize further, you can run these in parallel with asyncio.gather.
    for tf, (alpaca_tf, limit) in _TIMEFRAME_CONFIG.items():
        per_tf = await _fetch_alpaca_bars(
            client=client,
            symbols=symbols_norm,
            timeframe=alpaca_tf,
            limit=limit,
            rth_only=True,
        )

        for sym, candles in per_tf.items():
            # Only attach if we actually got candles
            if candles:
                out[sym][tf] = candles

    return out


# Optional: simple single-timeframe helper for backwards compatibility
async def fetch_candles(
    client: httpx.AsyncClient,
    symbol: str,
    interval: str = "5m",
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Backwards-compatible helper to fetch candles for a single symbol + timeframe.

    Uses the same Alpaca integration under the hood.

    Args:
        client: httpx.AsyncClient
        symbol: e.g. 'SPY'
        interval: one of '1m','3m','5m','15m','1h','1d','1w'
        limit: optional override for number of candles; if None, uses defaults.

    Returns:
        List of normalized candle dicts.
    """
    symbol_u = symbol.strip().upper()
    if interval not in _TIMEFRAME_CONFIG:
        raise ValueError(f"Unsupported interval '{interval}'. Valid: {list(_TIMEFRAME_CONFIG.keys())}")

    alpaca_tf, default_limit = _TIMEFRAME_CONFIG[interval]
    use_limit = limit if limit is not None else default_limit

    data = await _fetch_alpaca_bars(
        client=client,
        symbols=[symbol_u],
        timeframe=alpaca_tf,
        limit=use_limit,
        rth_only=True,
    )
    return data.get(symbol_u, [])
