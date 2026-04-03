import asyncio
import datetime as dt
import os
from typing import Dict, List, Any, Optional, AsyncIterator, Tuple

import httpx

try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    asyncpg = None  # type: ignore

from zoneinfo import ZoneInfo

# Optional external provider module (not required in simulation-only usage).
try:
    import market_data  # type: ignore
except Exception:  # pragma: no cover
    market_data = None  # type: ignore

EASTERN = ZoneInfo("America/New_York")

# All supported timeframes for API / storage
SUPPORTED_TFS = ["1m", "3m", "5m", "15m", "1h", "1d", "1w"]

# Seed limits per timeframe (RTH-only; seed_date is an "as-of" cutoff at 16:00 ET)
SEED_LIMITS = {
    "1m": 5000,
    "3m": 2500,
    "5m": 1500,
    "15m": 600,
    "1h": 400,
    "1d": 200,
    "1w": 50,
}

# Candle history tables (public schema)
CANDLE_TABLES = {
    "1m": "candle_history_1m",
    "3m": "candle_history_3m",
    "5m": "candle_history_5m",
    "15m": "candle_history_15m",
    "1h": "candle_history_1h",
    "1d": "candle_history_1d",
    "1w": "candle_history_1w",
}

# Alias used by simulation helpers that read from Supabase REST tables.
TF_TABLE = CANDLE_TABLES

RTH_SESSION = "rth"

# ---- Candle classification config -----------------------------------------

ATR_PERIOD = 14          # for mom_atr
VOL_LOOKBACK = 20        # for vol_rel

BODY_TINY = 0.10         # 10% of range
BODY_SMALL = 0.25        # 25% of range
BODY_LARGE = 0.60        # 60% of range

WICK_LONG = 0.60         # 60% of range
WICK_SHORT = 0.20        # 20% of range
WICK_DOMINANT = 2.0      # wick >= 2x body

VOL_HIGH = 2.0           # 2x avg volume
VOL_LOW = 0.5            # 0.5x avg volume


# ---- Supabase REST helpers -------------------------------------------------

def _sb_env() -> tuple[str, str]:
    url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
        or ""
    )
    if not url or not key:
        raise RuntimeError(
            "Missing SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY)."
        )
    return url, key


def _sb_headers(key: str) -> Dict[str, str]:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


async def _sb_get_rows(
    client: httpx.AsyncClient,
    table: str,
    params: Dict[str, str],
) -> List[Dict[str, Any]]:
    base_url, key = _sb_env()
    endpoint = f"{base_url}/rest/v1/{table}"
    r = await client.get(endpoint, headers=_sb_headers(key), params=params, timeout=30.0)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else ([data] if data else [])


# ---- RTH Helpers -----------------------------------------------------------

def is_rth_timestamp(ts_utc: dt.datetime) -> bool:
    """
    Return True if timestamp is within US RTH (Mon–Fri, 09:30–16:00 ET).

    Note:
      - Intraday bars are usually timestamped at their *start* (e.g. last 1m bar
        starts at 15:59 ET), so an exact 16:00 timestamp is uncommon intraday.
      - Higher timeframes (e.g. 1D/1W) are often timestamped exactly at the close
        (16:00:00 ET). We treat **exactly 16:00:00** as in-session so seed loads
        don't accidentally drop daily/weekly candles.
    """
    ts_et = ts_utc.astimezone(EASTERN)
    if ts_et.weekday() > 4:  # Sat/Sun
        return False
    t = ts_et.time()
    # 09:30 <= t <= 16:00 (exact close allowed)
    if t.hour < 9 or t.hour > 16:
        return False
    if t.hour == 9 and t.minute < 30:
        return False
    if t.hour == 16:
        # Allow exactly 16:00:00, reject anything after.
        return t.minute == 0 and t.second == 0
    return True


def is_rth_now() -> bool:
    """
    Decide whether the live update loop should run right now.
    We widen the window to:
      - Start:  09:25 ET
      - End:    16:05 ET

    IMPORTANT:
    We do NOT change is_rth_timestamp(ts), so individual candles
    are still filtered to strict RTH (09:30–16:00). This lets the
    engine run slightly early/late but still ignore pre/post-market data.
    """
    now_utc = dt.datetime.now(dt.timezone.utc)
    now_et = now_utc.astimezone(EASTERN)

    weekday = now_et.weekday()
    hour = now_et.hour
    minute = now_et.minute

    # Weekends → never run the loop
    if weekday >= 5:
        return False

    # Start running at 09:25 ET
    if hour < 9 or (hour == 9 and minute < 25):
        return False

    # Stop running at 16:05 ET
    if hour > 16 or (hour == 16 and minute > 5):
        return False

    return True



# ---- Bucket helpers for aggregation ---------------------------------------

def _bucket_start_3m(ts_et: dt.datetime) -> dt.datetime:
    minute = (ts_et.minute // 3) * 3
    return ts_et.replace(minute=minute, second=0, microsecond=0)


def _bucket_start_5m(ts_et: dt.datetime) -> dt.datetime:
    minute = (ts_et.minute // 5) * 5
    return ts_et.replace(minute=minute, second=0, microsecond=0)


def _bucket_start_15m(ts_et: dt.datetime) -> dt.datetime:
    minute = (ts_et.minute // 15) * 15
    return ts_et.replace(minute=minute, second=0, microsecond=0)


def _bucket_start_1h(ts_et: dt.datetime) -> dt.datetime:
    """
    Align hourly bars to 09:30, 10:30, 11:30, ...
    Using minutes since 09:30 as anchor.
    """
    open_minutes = 9 * 60 + 30
    total_minutes = ts_et.hour * 60 + ts_et.minute
    delta = max(0, total_minutes - open_minutes)
    bucket_index = delta // 60
    bucket_start_total = open_minutes + bucket_index * 60
    hour = bucket_start_total // 60
    minute = bucket_start_total % 60
    return ts_et.replace(hour=hour, minute=minute, second=0, microsecond=0)


def bucket_start(ts_utc: dt.datetime, tf: str) -> dt.datetime:
    """
    Given a UTC timestamp and timeframe key ('3m','5m','15m','1h'),
    return the bucket start as a UTC datetime.
    """
    ts_et = ts_utc.astimezone(EASTERN)
    if tf == "3m":
        start_et = _bucket_start_3m(ts_et)
    elif tf == "5m":
        start_et = _bucket_start_5m(ts_et)
    elif tf == "15m":
        start_et = _bucket_start_15m(ts_et)
    elif tf == "1h":
        start_et = _bucket_start_1h(ts_et)
    else:
        raise ValueError(f"Unsupported bucket timeframe: {tf}")
    return start_et.astimezone(dt.timezone.utc)


def tf_to_timedelta(tf: str) -> Optional[dt.timedelta]:
    """
    Convert a timeframe string like '1m', '3m', '5m', '15m', '1h', '1d'
    into a datetime.timedelta. If we can't parse it, return None.
    """
    if not tf:
        return None

    s = str(tf).strip().lower()

    # Explicit suffixes
    if s.endswith("m"):  # minutes
        try:
            mins = int(s[:-1])
            return dt.timedelta(minutes=mins)
        except ValueError:
            return None

    if s.endswith("h"):  # hours
        try:
            hours = int(s[:-1])
            return dt.timedelta(hours=hours)
        except ValueError:
            return None

    if s.endswith("d"):  # days
        try:
            days = int(s[:-1])
            return dt.timedelta(days=days)
        except ValueError:
            return None

    # Plain digits → treat as minutes (e.g. "5", "15", "60")
    if s.isdigit():
        try:
            mins = int(s)
            return dt.timedelta(minutes=mins)
        except ValueError:
            return None

    return None






class CandleEngine:
    """
    Maintains RTH-only candles for multiple symbols and timeframes, using:
      - DB seed history (multi-tf)
      - DB 1m playback stream + internal aggregation

    Simulation-only (no live loops, no external providers, no DB writes).
    """

    def __init__(
        self,
        symbols: List[str],
        twelve_data_api_key: Optional[str] = None,
        **_compat: Any,
    ):
        """
        Simulation CandleEngine (DB-backed, deterministic stepper).

        - Seed candles are loaded directly from DB for all supported timeframes, up to seed_date (as-of 16:00 ET).
        - Playback streams 1m RTH candles from DB and aggregates 3m/5m/15m/1h on the fly.
        - No external market data providers. No live loops. No DB writes.
        """
        # Back-compat: CandleEngine(td_key, symbols)
        if isinstance(symbols, str):
            td_key = symbols
            symbols_list = twelve_data_api_key if isinstance(twelve_data_api_key, list) else []
            self.td_api_key = td_key
            self.symbols = sorted({str(s).upper() for s in symbols_list if str(s).strip()})
        else:
            self.td_api_key = twelve_data_api_key or ""
            self.symbols = sorted({str(s).upper() for s in symbols if str(s).strip()})

        # Supabase REST (simulation playback)
        self._sb_url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
        self._sb_key = (
            os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_SERVICE_KEY")
            or os.getenv("SUPABASE_KEY")
            or ""
        )

        # Schema: one candle table per timeframe
        self._candle_tables: Dict[str, str] = {
            "1m": "candle_history_1m",
            "3m": "candle_history_3m",
            "5m": "candle_history_5m",
            "15m": "candle_history_15m",
            "1h": "candle_history_1h",
            "1d": "candle_history_1d",
            "1w": "candle_history_1w",
        }

        # candles[symbol][tf] -> list of enriched candle dicts
        self.candles: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        # latest_ts[symbol][tf] -> dt.datetime of last candle
        self.latest_ts: Dict[str, Dict[str, Optional[dt.datetime]]] = {}

        # Simulation playback state
        self.sim_clock_ts: Optional[dt.datetime] = None
        self._after_ts_utc: Optional[dt.datetime] = None
        self._day_buffer_1m: Dict[str, List[Dict[str, Any]]] = {}
        self._day_buffer_idx: Dict[str, int] = {}
        self._current_day_et: Dict[str, Optional[dt.date]] = {s: None for s in self.symbols}

        # ---- Diagnostics ----------------------------------------------------
        # Seed stats per symbol/timeframe
        self.seed_stats: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # Live emitted counts per symbol/timeframe
        self.live_emit_counts: Dict[str, Dict[str, int]] = {}
        # First/last emitted candle (overall)
        self.live_first_emitted: Dict[str, Dict[str, Any]] = {}
        self.live_last_emitted: Dict[str, Dict[str, Any]] = {}

        # ---- Micro-cluster (last few candles behavior) ------------------------

    # ----------------------------- Simulation REST API -----------------------

    def _sb_headers(self) -> Dict[str, str]:
        if not self._sb_url or not self._sb_key:
            raise RuntimeError("Missing SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY).")
        return {
            "apikey": self._sb_key,
            "Authorization": f"Bearer {self._sb_key}",
            "Content-Type": "application/json",
        }

    async def _sb_fetch_candles(
        self,
        *,
        table: str,
        symbol: str,
        ts_gte: Optional[str] = None,
        ts_lte: Optional[str] = None,
        session_eq: Optional[str] = None,
        limit: int = 5000,
        order_asc: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Fetch candles from a Supabase table via PostgREST.
        Table schema expected:
          symbol, ts, session, open, high, low, close, volume, vwap, trade_count
        """
        endpoint = f"{self._sb_url}/rest/v1/{table}"

        # PostgREST/Supabase commonly caps single responses (~1000 rows).
        # Paginate with offset until we collect `limit` rows or run out.
        target = max(0, int(limit))
        if target == 0:
            return []

        page_size = min(1000, target)
        base_params: List[Tuple[str, str]] = [
            ("select", "*"),
            ("symbol", f"eq.{symbol.upper()}"),
            ("order", f"ts.{'asc' if order_asc else 'desc'}"),
        ]
        if ts_gte:
            base_params.append(("ts", f"gte.{ts_gte}"))
        if ts_lte:
            base_params.append(("ts", f"lte.{ts_lte}"))
        if session_eq:
            base_params.append(("session", f"eq.{session_eq}"))

        out: List[Dict[str, Any]] = []
        offset = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            while len(out) < target:
                remaining = target - len(out)
                this_page = min(page_size, remaining)
                params = list(base_params)
                params.append(("limit", str(int(this_page))))
                params.append(("offset", str(int(offset))))

                r = await client.get(endpoint, headers=self._sb_headers(), params=params)
                r.raise_for_status()
                data = r.json()
                if not isinstance(data, list) or not data:
                    break

                out.extend(data)
                # If we got fewer than requested, no more pages.
                if len(data) < this_page:
                    break
                offset += len(data)

        return out[:target]

    @staticmethod
    def _parse_ts_utc(row_ts: Any) -> dt.datetime:
        if isinstance(row_ts, dt.datetime):
            return row_ts if row_ts.tzinfo else row_ts.replace(tzinfo=dt.timezone.utc)
        s = str(row_ts).replace("Z", "+00:00")
        t = dt.datetime.fromisoformat(s)
        return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)

    @staticmethod
    def _enrich_row(row: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(row)
        ts_utc = CandleEngine._parse_ts_utc(out.get("ts"))
        ts_et = ts_utc.astimezone(EASTERN)
        out["ts"] = ts_utc.isoformat()
        out["ts_et"] = ts_et.isoformat()
        out["date_et"] = ts_et.date().isoformat()
        return out

    async def load_seed_from_db(
        self,
        *,
        symbol: str,
        seed_date_et: str,
        counts: Optional[Dict[str, int]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Load seed candles up to seed_date (ET close 16:00).
        Uses per-timeframe tables: candle_history_1m, _3m, _5m, _15m, _1h, _1d, _1w.
        """
        counts = counts or {}
        sym = symbol.upper()

        seed_day = dt.date.fromisoformat(seed_date_et)
        cutoff_et = dt.datetime(seed_day.year, seed_day.month, seed_day.day, 16, 0, tzinfo=EASTERN)
        cutoff_utc = cutoff_et.astimezone(dt.timezone.utc).isoformat()

        # Ensure containers exist
        self.candles.setdefault(sym, {})
        self.latest_ts.setdefault(sym, {})

        def _norm_db_row(r: Dict[str, Any]) -> Dict[str, Any]:
            """
            Normalize a DB row into a raw candle dict consumable by _enrich_candle().
            Preserve optional fields if present (vwap, trade_count, etc).
            """
            out_r = dict(r)
            ts_utc = CandleEngine._parse_ts_utc(out_r.get("ts"))
            out_r["ts"] = ts_utc.isoformat()

            # Normalize numeric types
            if out_r.get("open") is not None:
                out_r["open"] = float(out_r["open"])
            if out_r.get("high") is not None:
                out_r["high"] = float(out_r["high"])
            if out_r.get("low") is not None:
                out_r["low"] = float(out_r["low"])
            if out_r.get("close") is not None:
                out_r["close"] = float(out_r["close"])
            if "volume" in out_r:
                out_r["volume"] = float(out_r.get("volume") or 0.0)
            if out_r.get("vwap") is not None:
                out_r["vwap"] = float(out_r["vwap"])
            if out_r.get("trade_count") is not None:
                try:
                    out_r["trade_count"] = int(out_r["trade_count"])
                except Exception:
                    pass
            out_r["symbol"] = sym
            return out_r

        out: Dict[str, List[Dict[str, Any]]] = {}
        for tf, table in self._candle_tables.items():
            lim = int(counts.get(tf, 5000))
            # Seed: only 1m table may include non-RTH rows; filter in DB so
            # descending LIMIT yields the intended number of RTH candles.
            session_eq = "rth" if tf == "1m" else None
            # IMPORTANT:
            # We want the seed to END at the cutoff (seed_date 16:00 ET) and go BACK in time.
            # So we fetch DESC (most recent first) with a LIMIT, then reverse to ASC in memory.
            rows_desc = await self._sb_fetch_candles(
                table=table,
                symbol=sym,
                ts_lte=cutoff_utc,
                session_eq=session_eq,
                limit=lim,
                order_asc=False,
            )
            rows = list(reversed(rows_desc))
            # Build fully enriched candles in chronological order for correct
            # context-dependent fields (ATR/vol_rel/cluster/swings/etc).
            self.candles[sym][tf] = []
            enriched_rows: List[Dict[str, Any]] = []
            for r in rows:
                raw = _norm_db_row(r)
                e = self._enrich_candle(sym, tf, raw)
                # Keep intraday seed candles aligned to RTH-only simulation.
                if tf in ("1m", "3m", "5m", "15m", "1h") and e.get("session") != "rth":
                    continue
                enriched_rows.append(e)
                self.candles[sym][tf].append(e)

            out[tf] = enriched_rows
            self.latest_ts[sym][tf] = (
                dt.datetime.fromisoformat(enriched_rows[-1]["ts"])
                if enriched_rows
                else None
            )

        # ---- Diagnostics: Seed phase stats ---------------------------------
        seed_tf_stats: Dict[str, Dict[str, Any]] = {}
        for tf in self._candle_tables.keys():
            arr = out.get(tf) or []
            n = len(arr)
            first_ts = arr[0].get("ts") if n else None
            last_ts = arr[-1].get("ts") if n else None
            seed_tf_stats[tf] = {"n": n, "first_ts": first_ts, "last_ts": last_ts}
        self.seed_stats[sym] = seed_tf_stats

        return out

    # ---- Diagnostics getters -----------------------------------------------
    def get_seed_stats(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        return self.seed_stats.get(symbol.upper(), {})

    def get_live_emit_counts(self, symbol: str) -> Dict[str, int]:
        return self.live_emit_counts.get(symbol.upper(), {})

    def get_live_first_last(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        sym = symbol.upper()
        return {
            "first": self.live_first_emitted.get(sym, {}),
            "last": self.live_last_emitted.get(sym, {}),
        }

    async def get_sim_days(self, *, symbol: str, start_after_seed_date_et: str, num_days: int) -> List[str]:
        """Return next N weekday dates after seed_date (ET)."""
        d = dt.date.fromisoformat(start_after_seed_date_et) + dt.timedelta(days=1)
        out: List[str] = []
        while len(out) < int(num_days):
            if d.weekday() <= 4:
                out.append(d.isoformat())
            d += dt.timedelta(days=1)
        return out

    async def stream_day(self, *, symbol: str, date_et: str) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream candles for one ET date.
        For simulation we stream *closed* candles from the DB tables.

        Events emitted:
          {"tf": "<tf>", "candle": {...}}
        """
        sym = symbol.upper()
        d = dt.date.fromisoformat(date_et)
        # RTH-only simulation window.
        start_et = dt.datetime(d.year, d.month, d.day, 9, 30, tzinfo=EASTERN)
        end_et = dt.datetime(d.year, d.month, d.day, 16, 0, tzinfo=EASTERN)
        start_utc = start_et.astimezone(dt.timezone.utc).isoformat()
        end_utc = end_et.astimezone(dt.timezone.utc).isoformat()

        # Ensure containers exist so enrichment/aggregation has state
        self.candles.setdefault(sym, {})
        self.latest_ts.setdefault(sym, {})
        self.candles[sym].setdefault("1m", [])
        self.latest_ts[sym].setdefault("1m", None)
        for tf in ("3m", "5m", "15m", "1h"):
            self.candles[sym].setdefault(tf, [])
            self.latest_ts[sym].setdefault(tf, None)

        def _norm_db_row(r: Dict[str, Any]) -> Dict[str, Any]:
            out_r = dict(r)
            ts_utc = CandleEngine._parse_ts_utc(out_r.get("ts"))
            out_r["ts"] = ts_utc.isoformat()
            if out_r.get("open") is not None:
                out_r["open"] = float(out_r["open"])
            if out_r.get("high") is not None:
                out_r["high"] = float(out_r["high"])
            if out_r.get("low") is not None:
                out_r["low"] = float(out_r["low"])
            if out_r.get("close") is not None:
                out_r["close"] = float(out_r["close"])
            if "volume" in out_r:
                out_r["volume"] = float(out_r.get("volume") or 0.0)
            if out_r.get("vwap") is not None:
                out_r["vwap"] = float(out_r["vwap"])
            if out_r.get("trade_count") is not None:
                try:
                    out_r["trade_count"] = int(out_r["trade_count"])
                except Exception:
                    pass
            out_r["symbol"] = sym
            return out_r

        # Live-sim behavior (match live bot):
        # - ingest new 1m candles
        # - enrich each 1m candle via _enrich_candle()
        # - aggregate HTFs (3m/5m/15m/1h) from 1m via _aggregate_from_1m()

        rows_1m = await self._sb_fetch_candles(
            table=self._candle_tables["1m"],
            symbol=sym,
            ts_gte=start_utc,
            ts_lte=end_utc,
            limit=25000,
            order_asc=True,
        )

        # Ensure diagnostics container exists
        self.live_emit_counts.setdefault(sym, {})

        def _diag_record_emit(tf: str, c: Dict[str, Any]) -> None:
            self.live_emit_counts[sym][tf] = int(self.live_emit_counts[sym].get(tf, 0)) + 1
            if sym not in self.live_first_emitted:
                self.live_first_emitted[sym] = {"tf": tf, "ts": c.get("ts"), "candle": c}
            self.live_last_emitted[sym] = {"tf": tf, "ts": c.get("ts"), "candle": c}

        # Keep last seen 1m (for end-of-day partial flush)
        last_1m: Optional[Dict[str, Any]] = None

        for r in rows_1m:
            raw_1m = _norm_db_row(r)
            e_1m = self._enrich_candle(sym, "1m", raw_1m)
            # Defensive filter in case non-RTH rows are returned in the ET range.
            if e_1m.get("session") != "rth":
                continue
            self.candles[sym]["1m"].append(e_1m)
            self.latest_ts[sym]["1m"] = dt.datetime.fromisoformat(e_1m["ts"])
            last_1m = e_1m

            # Emit 1m first (same as live)
            _diag_record_emit("1m", e_1m)
            yield {"tf": "1m", "candle": e_1m}

            # Aggregate + emit HTFs that close on this 1m step
            for tf in ("3m", "5m", "15m", "1h"):
                new_htf = self._aggregate_from_1m(sym, tf, [e_1m])
                for c in new_htf:
                    _diag_record_emit(tf, c)
                    yield {"tf": tf, "candle": c}

        # ---- EOD flush: emit last partial 1h candle (as full "1h") ----------
        # Live bot behavior: keep and forward the last partial hour (e.g., 15:30–16:00 ET).
        try:
            if last_1m is not None:
                # Compute the 1h bucket start anchored to 09:30 ET.
                # We flush the bucket that contains (end_et - 1 minute).
                last_et = dt.datetime.fromisoformat(last_1m["ts_et"])
                anchor = last_et.replace(hour=9, minute=30, second=0, microsecond=0)
                if last_et < anchor:
                    # Shouldn't happen in RTH-only, but guard anyway.
                    pass
                else:
                    delta_min = int((last_et - anchor).total_seconds() // 60)
                    bucket_start_et = anchor + dt.timedelta(minutes=(delta_min // 60) * 60)

                    # Collect 1m candles in the current (possibly partial) 1h bucket.
                    # Use ts_et for precise ET bucketing.
                    bucket_1m = []
                    for c in reversed(self.candles[sym].get("1m", [])):
                        ts_et = dt.datetime.fromisoformat(c.get("ts_et"))
                        if ts_et < bucket_start_et:
                            break
                        # only same day + rth (defensive)
                        if c.get("session") == "rth":
                            bucket_1m.append(c)
                    bucket_1m.reverse()

                    # Only emit if we have data and it's not already a closed 1h candle for this bucket.
                    if bucket_1m:
                        o = bucket_1m[0]["open"]
                        h = max(x["high"] for x in bucket_1m)
                        l = min(x["low"] for x in bucket_1m)
                        cl = bucket_1m[-1]["close"]
                        v = sum(float(x.get("volume") or 0) for x in bucket_1m)

                        # Timestamp convention for 1h candles is bucket START
                        # (09:30, 10:30, ... 15:30 ET).
                        bucket_start_utc = bucket_start_et.astimezone(dt.timezone.utc)
                        raw_1h = {
                            "ts": bucket_start_utc.isoformat(),
                            "open": o,
                            "high": h,
                            "low": l,
                            "close": cl,
                            "volume": v,
                        }
                        e_1h = self._enrich_candle(sym, "1h", raw_1h)

                        # Avoid duplicates: don't emit if last existing 1h has same ts.
                        last_existing = None
                        if self.candles.get(sym, {}).get("1h"):
                            last_existing = self.candles[sym]["1h"][-1].get("ts")
                        if last_existing != e_1h.get("ts"):
                            self.candles[sym].setdefault("1h", []).append(e_1h)
                            self.latest_ts[sym]["1h"] = dt.datetime.fromisoformat(e_1h["ts"])
                            _diag_record_emit("1h", e_1h)
                            yield {"tf": "1h", "candle": e_1h}
        except Exception:
            # Never let diagnostics/flush break the sim loop.
            pass

    def _compute_cluster(
        self,
        window_candles: List[Dict[str, Any]],
        target_lookback: int = 5,
    ) -> Dict[str, Any]:
        """
        Compute a 'cluster' summary describing how the last few candles behaved.
        Returns fields:
            bull_count, bear_count, net_change_pct,
            avg_mom_atr, avg_vol_rel, state
        """
        if not window_candles:
            return {
                "lookback": 0,
                "bull_count": 0,
                "bear_count": 0,
                "net_change_pct": None,
                "avg_mom_atr": None,
                "avg_vol_rel": None,
                "state": "neutral",
            }

        lookback = min(target_lookback, len(window_candles))
        candles = window_candles[-lookback:]

        bull_count = 0
        bear_count = 0
        closes = []
        mom_vals = []
        vol_vals = []

        for c in candles:
            o = float(c["open"])
            cl = float(c["close"])
            closes.append(cl)

            if cl > o:
                bull_count += 1
            elif cl < o:
                bear_count += 1

            mr = c.get("mom_atr")
            if mr is not None:
                try:
                    mom_vals.append(float(mr))
                except:
                    pass

            vr = c.get("vol_rel")
            if vr is not None:
                try:
                    vol_vals.append(float(vr))
                except:
                    pass

        net_change_pct = None
        if len(closes) >= 2 and closes[0] != 0:
            net_change_pct = (closes[-1] - closes[0]) / closes[0]

        avg_mom_atr = sum(mom_vals) / len(mom_vals) if mom_vals else None
        avg_vol_rel = sum(vol_vals) / len(vol_vals) if vol_vals else None

        # ---- Classification thresholds ----
        thr_momo = 0.003
        thr_chop = 0.002
        thr_range = 0.0015
        trend_ratio = 0.6

        bull_ratio = bull_count / lookback
        bear_ratio = bear_count / lookback

        state = "neutral"
        pos = lambda x: x is not None and x > 0
        neg = lambda x: x is not None and x < 0

        if (
            net_change_pct is not None
            and net_change_pct > thr_momo
            and bull_ratio >= trend_ratio
            and pos(avg_mom_atr)
        ):
            state = "bull_momo"

        elif (
            net_change_pct is not None
            and net_change_pct < -thr_momo
            and bear_ratio >= trend_ratio
            and neg(avg_mom_atr)
        ):
            state = "bear_momo"

        elif net_change_pct is not None:
            low_vol = avg_vol_rel is not None and avg_vol_rel < 1.0
            very_low_vol = avg_vol_rel is not None and avg_vol_rel < 0.7

            if abs(net_change_pct) < thr_range and very_low_vol:
                state = "range_low_vol"
            elif abs(net_change_pct) < thr_chop:
                state = "chop"
            elif net_change_pct > 0 and low_vol:
                state = "bull_drift"
            elif net_change_pct < 0 and low_vol:
                state = "bear_drift"
            else:
                state = "neutral"

        return {
            "lookback": lookback,
            "bull_count": bull_count,
            "bear_count": bear_count,
            "net_change_pct": net_change_pct,
            "avg_mom_atr": avg_mom_atr,
            "avg_vol_rel": avg_vol_rel,
            "state": state,
        }



    def _get_last_closed_candle(
        self,
        symbol: str,
        timeframe: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Return the last *closed* candle for (symbol, timeframe).

        Logic:
          - If no candles → None
          - For 1d / 1w, we assume all loaded candles are closed → return last
          - For intraday TFs:
              * Use tf_to_timedelta to get the bar duration
              * If now >= ts + duration → last candle is closed
              * Else, fall back to previous candle if it exists
        """
        sym = symbol.upper()
        tf_candles = self.candles.get(sym, {}).get(timeframe, [])
        if not tf_candles:
            return None

        # Daily / weekly: all bars from Alpaca / history are closed
        if timeframe in ("1d", "1w"):
            return tf_candles[-1]

        delta = tf_to_timedelta(timeframe)
        if delta is None:
            # Unknown format; just return the last one as-is
            return tf_candles[-1]

        now_utc = self.sim_clock_ts or dt.datetime.now(dt.timezone.utc)

        last = tf_candles[-1]
        try:
            ts = dt.datetime.fromisoformat(last["ts"])
        except Exception:
            # If ts is weird, just treat it as closed
            return last

        # If current time has passed the end of this bar, it's closed.
        if now_utc >= ts + delta:
            return last

        # Otherwise, this bar is still forming → use previous if available
        if len(tf_candles) >= 2:
            return tf_candles[-2]

        # No previous bar: nothing closed yet
        return None




    

    # ---- Candle enrichment / builder --------------------------------------
    def _enrich_candle(
        self,
        symbol: str,
        timeframe: str,
        raw: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Take a raw OHLCV candle dict (ts, open, high, low, close, volume),
        compute all derived fields (geometry, shape, volume context, momentum,
        micro-structure) and return the enriched candle.
        """

        # --- timestamp handling: keep UTC as primary, add ET helpers ----
        ts_raw = raw.get("ts")
        ts: dt.datetime

        if isinstance(ts_raw, dt.datetime):
            ts = ts_raw
        elif isinstance(ts_raw, str):
            try:
                ts = dt.datetime.fromisoformat(ts_raw)
            except Exception:
                ts = dt.datetime.now(dt.timezone.utc)
        else:
            ts = dt.datetime.now(dt.timezone.utc)

        # Make sure it's timezone-aware (assume UTC if naive)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)

        # View in US/Eastern for readability
        ts_et = ts.astimezone(EASTERN)

        # --- core OHLCV fields ----------------------------------------------
        o = float(raw["open"])
        h = float(raw["high"])
        l = float(raw["low"])
        c = float(raw["close"])
        v = float(raw.get("volume", 0.0))

        # Base list of existing candles for context
        symbol_candles = self.candles.get(symbol, {})
        tf_candles: List[Dict[str, Any]] = symbol_candles.get(timeframe, [])

        # --- basic geometry ---
        range_ = max(h - l, 0.0)
        body = abs(c - o)
        upper_wick = max(h - max(o, c), 0.0)
        lower_wick = max(min(o, c) - l, 0.0)
        spread_strength = (body / range_) if range_ > 0 else 0.0

        # direction
        eps = 1e-8
        if c > o + eps:
            direction = "bull"
        elif c < o - eps:
            direction = "bear"
        else:
            direction = "neutral"

        # --- previous candle context ---
        prev_candle: Optional[Dict[str, Any]] = tf_candles[-1] if tf_candles else None
        prev_close = float(prev_candle["close"]) if prev_candle else c
        prev_high = float(prev_candle["high"]) if prev_candle else h
        prev_low = float(prev_candle["low"]) if prev_candle else l

        # --- swing flags ---
        is_higher_high = h > prev_high if prev_candle else False
        is_lower_high = h < prev_high if prev_candle else False
        is_higher_low = l > prev_low if prev_candle else False
        is_lower_low = l < prev_low if prev_candle else False

        if prev_candle:
            if is_higher_high and is_higher_low:
                swing_type: Optional[str] = "HH"
            elif is_lower_high and is_lower_low:
                swing_type = "LL"
            elif is_higher_low and not is_higher_high and not is_lower_high:
                swing_type = "HL"
            elif is_lower_high and not is_lower_low and not is_higher_low:
                swing_type = "LH"
            else:
                swing_type = None
        else:
            swing_type = None

        # --- volume context ---
        vol_rel = 1.0
        is_high_vol = False
        is_low_vol = False
        if tf_candles:
            vols = [float(cand.get("volume", 0.0)) for cand in tf_candles[-VOL_LOOKBACK:]]
            avg_vol = sum(vols) / len(vols) if vols else 0.0
            if avg_vol > 0:
                vol_rel = v / avg_vol
                is_high_vol = vol_rel >= VOL_HIGH
                is_low_vol = vol_rel <= VOL_LOW

        # --- ATR-based momentum ---
        mom_raw = c - prev_close
        atr = 0.0
        if len(tf_candles) >= 2:
            start_idx = max(1, len(tf_candles) - ATR_PERIOD)
            trs: List[float] = []
            for i in range(start_idx, len(tf_candles)):
                cur = tf_candles[i]
                prev = tf_candles[i - 1]
                ch = float(cur["high"])
                cl = float(cur["low"])
                pc = float(prev["close"])
                tr = max(ch - cl, abs(ch - pc), abs(cl - pc))
                trs.append(tr)
            atr = (sum(trs) / len(trs)) if trs else 0.0

        mom_atr = (mom_raw / atr) if atr > 0 else 0.0

        # --- shape classification ---

        def classify_shape() -> str:
            if range_ <= 0:
                return "normal"

            body_ratio = (body / range_) if range_ > 0 else 0.0
            upper_ratio = (upper_wick / range_) if range_ > 0 else 0.0
            lower_ratio = (lower_wick / range_) if range_ > 0 else 0.0

            # 1) marubozu
            if body_ratio >= BODY_LARGE and upper_ratio <= WICK_SHORT and lower_ratio <= WICK_SHORT:
                if direction == "bull":
                    return "marubozu_bull"
                if direction == "bear":
                    return "marubozu_bear"

            # 2) hammer / inverted / shooting star
            if lower_ratio >= WICK_LONG and upper_ratio <= WICK_SHORT and body_ratio <= BODY_SMALL:
                # body near top half
                if min(o, c) >= l + 0.5 * range_:
                    return "hammer"

            if upper_ratio >= WICK_LONG and lower_ratio <= WICK_SHORT and body_ratio <= BODY_SMALL:
                if direction == "bear":
                    return "shooting_star"
                else:
                    return "inverted_hammer"

            # 3) doji (small body)
            if body_ratio <= BODY_TINY:
                return "doji"

            # 4) long wicks
            if upper_ratio >= WICK_LONG and upper_wick >= WICK_DOMINANT * body:
                return "long_upper_wick"
            if lower_ratio >= WICK_LONG and lower_wick >= WICK_DOMINANT * body:
                return "long_lower_wick"

            # 5) small body
            if body_ratio <= BODY_SMALL:
                return "small_body"

            return "normal"

        shape = classify_shape()


        enriched = dict(raw)
        enriched.update({
            "ts": ts.isoformat(),
            "tf": timeframe,
            "ts_et": ts_et.isoformat(),
            "date_et": ts_et.date().isoformat(),
            "time_et": ts_et.strftime("%H:%M:%S"),
        
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v,
            "body": body,
            "range": range_,
            "upper_wick": upper_wick,
            "lower_wick": lower_wick,
            "direction": direction,
            "spread_strength": spread_strength,
            "shape": shape,
            "vol_rel": vol_rel,
            "is_high_vol": is_high_vol,
            "is_low_vol": is_low_vol,
            "mom_raw": mom_raw,
            "mom_atr": mom_atr,
            "is_higher_high": is_higher_high,
            "is_lower_high": is_lower_high,
            "is_higher_low": is_higher_low,
            "is_lower_low": is_lower_low,
            "swing_type": swing_type,
        })
        
        # ---------- NEW BLOCK: cluster summary ----------
        try:
            # previous TF candles + this one
            window = (tf_candles[-4:] if len(tf_candles) >= 4 else tf_candles[:]) + [enriched]
            cluster = self._compute_cluster(window)
            enriched["cluster"] = cluster
        except Exception as e:
            enriched["cluster"] = {
                "lookback": 0,
                "bull_count": 0,
                "bear_count": 0,
                "net_change_pct": None,
                "avg_mom_atr": None,
                "avg_vol_rel": None,
                "state": "neutral",
            }
        # --------------------------------------------------
        
        return enriched




    def _aggregate_from_1m(
        self,
        symbol: str,
        target_tf: str,
        new_1m: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Aggregate newly added 1m candles into higher timeframe candles (3m,5m,15m,1h).

        IMPORTANT:
        - We only build a higher-TF candle if the underlying 1m bucket is
          COMPLETE, i.e. we have all required 1m bars for that bucket.
        - For example, a 3m bucket starting at 10:51 requires 1m bars at
          10:51, 10:52, 10:53 (in UTC minute steps).
        - This prevents partially built 3m/5m candles when Twelve Data is
          delayed in delivering some 1m bars.
        """
        if target_tf not in ("3m", "5m", "15m", "1h"):
            return []

        # Ensure containers exist
        self.candles.setdefault(symbol, {})
        self.candles[symbol].setdefault(target_tf, [])
        self.latest_ts.setdefault(symbol, {})
        self.latest_ts[symbol].setdefault(target_tf, None)

        existing = self.candles[symbol][target_tf]
        last_ts = self.latest_ts[symbol][target_tf]

        # All known 1m candles for this symbol
        all_1m = self.candles[symbol].get("1m", [])
        if not all_1m:
            return []

        # Determine earliest ts to consider:
        #   - if we already have higher-TF candles, start from their last ts
        #   - otherwise, start from the first 1m ts
        if last_ts is not None:
            start_ts = last_ts
        else:
            start_ts = dt.datetime.fromisoformat(all_1m[0]["ts"])

        # Collect 1m candles with ts >= start_ts
        relevant_1m = [
            c for c in all_1m if dt.datetime.fromisoformat(c["ts"]) >= start_ts
        ]
        if not relevant_1m:
            return []

        # Group relevant 1m candles into buckets by target_tf
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for c in relevant_1m:
            ts = dt.datetime.fromisoformat(c["ts"])
            b_start = bucket_start(ts, target_tf)  # UTC bucket start
            key = b_start.isoformat()
            buckets.setdefault(key, []).append(c)

        new_htf: List[Dict[str, Any]] = []

        # Determine how many 1m candles are required per bar for this timeframe
        delta = tf_to_timedelta(target_tf)
        # Fallback: if we can't parse the timeframe, behave like old logic
        if delta is None or delta.total_seconds() <= 0:
            required_1m_count = None
        else:
            # e.g. "3m" -> 3, "5m" -> 5, "15m" -> 15, "1h" -> 60
            required_1m_count = int(delta.total_seconds() // 60) or 1

        for b_ts_str, group in sorted(buckets.items(), key=lambda kv: kv[0]):
            b_ts = dt.datetime.fromisoformat(b_ts_str)

            # Never rebuild or go backwards: only build buckets AFTER last_ts
            if last_ts is not None and b_ts <= last_ts:
                continue

            # If we don't know how many 1m bars are required (shouldn't happen
            # for '3m','5m','15m','1h'), fall back to old behavior.
            if required_1m_count is None:
                opens = [g["open"] for g in group]
                highs = [g["high"] for g in group]
                lows = [g["low"] for g in group]
                closes = [g["close"] for g in group]
                vols = [g.get("volume", 0.0) for g in group]

                raw_candle = {
                    "ts": b_ts_str,
                    "open": float(opens[0]),
                    "high": float(max(highs)),
                    "low": float(min(lows)),
                    "close": float(closes[-1]),
                    "volume": float(sum(vols)),
                }
                enriched = self._enrich_candle(symbol, target_tf, raw_candle)
                existing.append(enriched)
                self.latest_ts[symbol][target_tf] = b_ts
                new_htf.append(enriched)
                continue

            # ---------- COMPLETENESS CHECK FOR THE BUCKET ----------
            #
            # A bucket is considered COMPLETE only if we have all expected 1m
            # timestamps:
            #   expected_ts[i] = b_ts + i * 1 minute, for i in [0, required_1m_count-1]
            #
            # This guarantees, for example, that a 3m bar at 10:51 has 1m bars
            # for 10:51, 10:52, 10:53 before we aggregate it.
            group_ts_set = {
                dt.datetime.fromisoformat(g["ts"]) for g in group
            }

            complete = True
            for i in range(required_1m_count):
                expected_ts = b_ts + dt.timedelta(minutes=i)
                if expected_ts not in group_ts_set:
                    complete = False
                    break

            if not complete:
                # Underlying 1m bars for this bucket are not all present yet.
                # We skip building this bar now; a later Twelve Data poll can
                # fill in missing minutes and then we'll aggregate it.
                continue

            # ---------- AGGREGATE COMPLETE BUCKET ----------
            # Now we know we have all required 1m candles for this bar.
            # We can safely build an OHLCV bar aligned with b_ts.

            # Sort group by ts to get correct open/close ordering
            group_sorted = sorted(
                group,
                key=lambda g: dt.datetime.fromisoformat(g["ts"]),
            )

            opens = [g["open"] for g in group_sorted]
            highs = [g["high"] for g in group_sorted]
            lows = [g["low"] for g in group_sorted]
            closes = [g["close"] for g in group_sorted]
            vols = [g.get("volume", 0.0) for g in group_sorted]

            raw_candle = {
                "ts": b_ts_str,
                "open": float(opens[0]),
                "high": float(max(highs)),
                "low": float(min(lows)),
                "close": float(closes[-1]),
                "volume": float(sum(vols)),
            }

            enriched = self._enrich_candle(symbol, target_tf, raw_candle)
            existing.append(enriched)
            self.latest_ts[symbol][target_tf] = b_ts
            new_htf.append(enriched)

        return new_htf


    def _aggregate_new_1m(self, symbol: str, new_1m: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Helper to aggregate newly added 1m candles to 3m,5m,15m,1h.
        Returns dict[tf] = list of new higher-TF candles.
        """
        result: Dict[str, List[Dict[str, Any]]] = {}
        for tf in ("3m", "5m", "15m", "1h"):
            new_htf = self._aggregate_from_1m(symbol, tf, new_1m)
            if new_htf:
                result[tf] = new_htf
        return result

    # ---------------- Simulation DB loaders ----------------

    @staticmethod
    def _seed_cutoff_utc(seed_date: dt.date) -> dt.datetime:
        """Return seed cutoff timestamp in UTC: seed_date 16:00 ET."""
        cutoff_et = dt.datetime.combine(seed_date, dt.time(16, 0), tzinfo=EASTERN)
        return cutoff_et.astimezone(dt.timezone.utc)

    async def _fetch_candles_asof(
        self,
        symbol: str,
        tf: str,
        asof_utc: dt.datetime,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Fetch last `limit` candles for timeframe `tf` with ts <= asof_utc."""
        table = CANDLE_TABLES[tf]
        if tf == "1m":
            sql = f"""
                SELECT symbol, ts, session, open, high, low, close, volume, vwap, trade_count
                FROM public.{table}
                WHERE symbol = $1
                  AND session = $2
                  AND ts <= $3
                ORDER BY ts DESC
                LIMIT {limit}
            """
            rows = await self.pool.fetch(sql, symbol, RTH_SESSION, asof_utc)
        else:
            sql = f"""
                SELECT symbol, ts, open, high, low, close, volume, vwap, trade_count
                FROM public.{table}
                WHERE symbol = $1
                  AND ts <= $2
                ORDER BY ts DESC
                LIMIT {limit}
            """
            rows = await self.pool.fetch(sql, symbol, asof_utc)

        candles = [dict(r) for r in rows][::-1]
        return candles

    async def load_seed(self, symbol: str, seed_date: dt.date) -> Dict[str, Any]:
        """
        Load seed candles (as-of seed_date 16:00 ET) for all supported timeframes.

        Returns seed payload for IndicatorBot.bootstrap().
        """
        symbol = symbol.upper().strip()
        if symbol not in self.symbols:
            self.symbols.append(symbol)
            self.symbols = sorted(set(self.symbols))
            self._current_day_et[symbol] = None

        asof_utc = self._seed_cutoff_utc(seed_date)

        self.candles.setdefault(symbol, {})
        self.latest_ts.setdefault(symbol, {})
        self._day_buffer_1m.setdefault(symbol, [])
        self._day_buffer_idx.setdefault(symbol, 0)

        seed_candles: Dict[str, List[Dict[str, Any]]] = {}

        for tf in SUPPORTED_TFS:
            limit = SEED_LIMITS.get(tf)
            if not limit:
                continue
            raw = await self._fetch_candles_asof(symbol, tf, asof_utc, limit)
            enriched: List[Dict[str, Any]] = []
            for c in raw:
                c_norm = {
                    "ts": c["ts"].isoformat() if isinstance(c["ts"], dt.datetime) else str(c["ts"]),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                    "volume": float(c.get("volume") or 0.0),
                }
                if "vwap" in c and c["vwap"] is not None:
                    c_norm["vwap"] = float(c["vwap"])
                if "trade_count" in c and c["trade_count"] is not None:
                    c_norm["trade_count"] = int(c["trade_count"])
                if "session" in c and c["session"] is not None:
                    c_norm["session"] = c["session"]
                e = self._enrich_candle(symbol, tf, c_norm)
                enriched.append(e)

            self.candles[symbol][tf] = enriched
            self.latest_ts[symbol][tf] = (
                dt.datetime.fromisoformat(enriched[-1]["ts"]) if enriched else None
            )
            seed_candles[tf] = enriched

        self.sim_clock_ts = self.latest_ts[symbol].get("1m")
        self._after_ts_utc = asof_utc
        self._current_day_et[symbol] = None
        self._day_buffer_1m[symbol] = []
        self._day_buffer_idx[symbol] = 0

        return {
            "symbol": symbol,
            "as_of": self.sim_clock_ts.isoformat() if self.sim_clock_ts else asof_utc.isoformat(),
            "candles": seed_candles,
        }

    async def _find_next_rth_1m_ts(self, symbol: str) -> Optional[dt.datetime]:
        """Find the next available RTH 1m candle timestamp after self._after_ts_utc."""
        if self._after_ts_utc is None:
            return None
        sql = f"""
            SELECT ts
            FROM public.{CANDLE_TABLES['1m']}
            WHERE symbol = $1
              AND session = $2
              AND ts > $3
            ORDER BY ts ASC
            LIMIT 1
        """
        row = await self.pool.fetchrow(sql, symbol, RTH_SESSION, self._after_ts_utc)
        return row["ts"] if row else None

    async def _load_next_day_buffer(self, symbol: str) -> bool:
        """
        Load next trading day's RTH 1m candles into buffer.
        Returns False if no more data is available.
        """
        next_ts = await self._find_next_rth_1m_ts(symbol)
        if not next_ts:
            return False

        day_et = (next_ts.astimezone(EASTERN)).date()
        sql = f"""
            SELECT symbol, ts, session, open, high, low, close, volume, vwap, trade_count
            FROM public.{CANDLE_TABLES['1m']}
            WHERE symbol = $1
              AND session = $2
              AND (ts AT TIME ZONE 'America/New_York')::date = $3
            ORDER BY ts ASC
        """
        rows = await self.pool.fetch(sql, symbol, RTH_SESSION, day_et)
        buf = [dict(r) for r in rows]

        self._day_buffer_1m[symbol] = buf
        self._day_buffer_idx[symbol] = 0
        self._current_day_et[symbol] = day_et

        if buf:
            self._after_ts_utc = buf[-1]["ts"]
        return True

    async def step(self, symbol: str) -> Dict[str, Any]:
        """
        Advance simulation by exactly one 1m candle for `symbol`.

        Returns event payload with newly produced candles.
        Raises StopAsyncIteration if no more candles are available.
        """
        symbol = symbol.upper().strip()
        if symbol not in self.candles:
            raise ValueError(f"Symbol not seeded: {symbol}. Call load_seed() first.")

        buf = self._day_buffer_1m.get(symbol, [])
        idx = self._day_buffer_idx.get(symbol, 0)
        if idx >= len(buf):
            ok = await self._load_next_day_buffer(symbol)
            if not ok:
                raise StopAsyncIteration(f"No more 1m candles available for {symbol}.")
            buf = self._day_buffer_1m[symbol]
            idx = 0

        raw = buf[idx]
        self._day_buffer_idx[symbol] = idx + 1

        c_norm = {
            "ts": raw["ts"].isoformat(),
            "open": float(raw["open"]),
            "high": float(raw["high"]),
            "low": float(raw["low"]),
            "close": float(raw["close"]),
            "volume": float(raw.get("volume") or 0.0),
            "session": raw.get("session"),
        }
        if raw.get("vwap") is not None:
            c_norm["vwap"] = float(raw["vwap"])
        if raw.get("trade_count") is not None:
            c_norm["trade_count"] = int(raw["trade_count"])

        e1 = self._enrich_candle(symbol, "1m", c_norm)
        self.candles[symbol]["1m"].append(e1)
        self.latest_ts[symbol]["1m"] = dt.datetime.fromisoformat(e1["ts"])

        self.sim_clock_ts = self.latest_ts[symbol]["1m"]

        new_payload: Dict[str, List[Dict[str, Any]]] = {
            "1m": [e1],
            "3m": [],
            "5m": [],
            "15m": [],
            "1h": [],
            "1d": [],
            "1w": [],
        }

        new_1m_list = [e1]
        for tf in ("3m", "5m", "15m", "1h"):
            produced = self._aggregate_from_1m(symbol, tf, new_1m_list)
            if produced:
                self.candles[symbol][tf].extend(produced)
                self.latest_ts[symbol][tf] = dt.datetime.fromisoformat(produced[-1]["ts"])
                new_payload[tf] = produced

        return {
            "symbol": symbol,
            "sim_clock": self.sim_clock_ts.isoformat() if self.sim_clock_ts else None,
            "new": new_payload,
        }

    def get_candles(self, symbol: str, timeframe: str) -> List[Dict[str, Any]]:
        """
        Return list of enriched candles for the given symbol+timeframe.
        """
        symbol = symbol.upper()
        if symbol not in self.candles:
            return []
        return self.candles[symbol].get(timeframe, [])
