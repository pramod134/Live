import os
import math
import asyncio
import json
import datetime as dt
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from candle_engine import CandleEngine, SUPPORTED_TFS

from indicator_calc1 import compute_all_indicators

from indicator_calc2 import compute_advanced_extras



from strategies import evaluate_strategies

from liquidity_pool_builder import build_liquidity_pool

# Events engine (NEW)
from spot_event import SpotEventContext, compute_spot_events

from fvg_pool import build_symbol_fvg_pool, upsert_symbol_fvg_pool






# NOTE:
# Do NOT override print() in the simulation indicator bot.
# We rely on stdout logs for end-to-end validation.

EASTERN = dt.timezone(dt.timedelta(hours=-5))  # not critical, mainly for consistency


# ---------------------------------------------------------------------------
# Supabase helper: update indicators columns in spot_tf
# ---------------------------------------------------------------------------


async def update_indicators_in_spot_tf(
    symbol: str,
    timeframe: str,
    trend: Dict[str, Any],
    pivots: Dict[str, Any],
    swings: Dict[str, Any],
    structural: Dict[str, Any],
    fvgs: List[Dict[str, Any]],
    liquidity: Dict[str, Any],
    volume_profile: Dict[str, Any],
    extras: Dict[str, Any],
    extras_advanced: Optional[Dict[str, Any]],
    structure_state: Optional[str],
    strategies: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Patch public.spot_tf for the given symbol+timeframe with the computed indicators.

    Uses Supabase REST. Requires:
      - SUPABASE_URL
      - SUPABASE_SERVICE_ROLE_KEY or SUPABASE_SERVICE_KEY or SUPABASE_KEY
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not supabase_url or not supabase_key:
        # print("[INDICATORS][DB] Skipping update: Supabase env vars missing")
        return

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_tf"
    params = {
        "symbol": f"eq.{symbol}",
        "timeframe": f"eq.{timeframe}",
    }
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "return=minimal",
    }

    payload: Dict[str, Any] = {
        "trend": trend,
        "pivots": pivots,
        "swings": swings,
        "structural": structural,
        "fvgs": fvgs,
        "liquidity": liquidity,
        "volume_profile": volume_profile,
        "extras": extras,
        "extras_advanced": extras_advanced,
        "last_updated": dt.datetime.now(dt.timezone.utc).isoformat(),
    }

    if structure_state is not None:
        payload["structure_state"] = structure_state
    if strategies is not None:
        payload["strategies"] = strategies

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.patch(
                endpoint,
                params=params,
                json=payload,
                headers=headers,
            )
            if resp.status_code >= 400:
                # print(
                # f"[INDICATORS][DB] Failed to update indicators for "
                # f"{symbol} {timeframe} (status={resp.status_code})"
                # )
                # print(f"[INDICATORS][DB] Response text: {resp.text}")
                resp.raise_for_status()
        except httpx.HTTPError as e:
            pass
            # print(f"[INDICATORS][DB] HTTP error for {symbol} {timeframe}: {e}")
        except Exception as e:
            pass
            # print(f"[INDICATORS][DB] Unexpected error for {symbol} {timeframe}: {e}")


async def fetch_spot_tf_rows_for_symbol(symbol: str) -> List[Dict[str, Any]]:
    """
    Fetch all spot_tf rows for a symbol across all timeframes.
    Used by ZoneFinder to build a unified (Option A) zone map.
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_* key env vars")

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_tf"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }

    params = {
        "symbol": f"eq.{symbol.upper()}",
        "select": "*",
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(endpoint, headers=headers, params=params)
        if r.status_code != 200:
            raise RuntimeError(f"spot_tf fetch failed ({r.status_code}): {r.text}")
        return r.json()



async def upsert_spot_liquidity_pool(
    symbol: str,
    asof: str,
    pool_version: str,
    levels: List[Dict[str, Any]],
    stats: Dict[str, Any],
    tol_price: Optional[float] = None,
) -> None:
    """Upsert current liquidity pool snapshot into public.spot_liquidity_pool."""
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not supabase_url or not supabase_key:
        # print("[LIQ_POOL][DB] Skipping upsert: Supabase env vars missing")
        return

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_liquidity_pool"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    payload: Dict[str, Any] = {
        "symbol": symbol.upper(),
        "asof": asof,
        "pool_version": pool_version,
        "levels": levels,
        "stats": stats,
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    if tol_price is not None:
        payload["tol_price"] = tol_price

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(endpoint, json=payload, headers=headers)
        if resp.status_code >= 400:
            # print(f"[LIQ_POOL][DB] Upsert failed for {symbol} (status={resp.status_code})")
            # print(f"[LIQ_POOL][DB] Response text: {resp.text}")
            resp.raise_for_status()


# ---------------------------------------------------------------------------
# Supabase helper: fetch liquidity pool row (for sweep detection)
# ---------------------------------------------------------------------------
async def fetch_spot_liquidity_pool_row(symbol: str) -> Optional[Dict[str, Any]]:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )
    if not supabase_url or not supabase_key:
        return None

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_liquidity_pool"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }
    params = {
        "symbol": f"eq.{symbol.upper()}",
        "select": "*",
        "order": "updated_at.desc",
        "limit": "1",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.get(endpoint, headers=headers, params=params)
            if r.status_code != 200:
                return None
            rows = r.json() or []
            return rows[0] if rows else None
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Supabase helper: upsert spot_events row (verification/audit)
# ---------------------------------------------------------------------------
async def upsert_spot_events_row(
    symbol: str,
    timeframe: str,
    events_latest: Dict[str, Any],
    events_active: Dict[str, Any],
    events_recent: List[Dict[str, Any]],
) -> None:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not supabase_url or not supabase_key:
        # print("[EVENTS][DB] Skipping upsert: Supabase env vars missing")
        return

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_events"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    payload: Dict[str, Any] = {
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "last_updated": dt.datetime.now(dt.timezone.utc).isoformat(),
        "events_latest": events_latest,
        "events_active": events_active,
        "events_recent": events_recent,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(endpoint, json=payload, headers=headers)
        if resp.status_code >= 400:
            # print(f"[EVENTS][DB] Upsert failed for {symbol} {timeframe} (status={resp.status_code})")
            # print(f"[EVENTS][DB] Response text: {resp.text}")
            resp.raise_for_status()







# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
async def needs_backfill_structure_state(symbol: str, timeframe: str) -> bool:
    """
    Return True if spot_tf.structure_state is NULL/empty for this symbol+timeframe.
    Used to force a one-time indicator backfill even when the last candle ts
    has not changed (e.g. after deleting/recreating spot_tf rows).
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not supabase_url or not supabase_key:
        # Can't check; don't force backfill in this case.
        return False

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/spot_tf"
    params = {
        "symbol": f"eq.{symbol}",
        "timeframe": f"eq.{timeframe}",
        "select": "structure_state",
    }
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(endpoint, params=params, headers=headers)
            resp.raise_for_status()
            rows = resp.json() or []
        except Exception as e:
            # print(
            # f"[INDICATORS][DB] Failed to check structure_state for "
            # f"{symbol} {timeframe}: {e}"
            # )
            return False

    if not rows:
        # No row → don't force backfill here (engine should create rows first).
        return False

    ss = rows[0].get("structure_state")

    # Treat NULL, empty string, or purely whitespace as "missing"
    if ss is None:
        return True
    if isinstance(ss, str) and not ss.strip():
        return True

    return False



def _ensure_dt(ts_raw: Any) -> dt.datetime:
    """
    Try to convert whatever 'ts' is into an aware UTC datetime.
    CandleEngine usually stores ISO strings or datetime already.
    """
    if isinstance(ts_raw, dt.datetime):
        ts = ts_raw
    elif isinstance(ts_raw, str):
        try:
            ts = dt.datetime.fromisoformat(ts_raw)
        except Exception:
            ts = dt.datetime.now(dt.timezone.utc)
    else:
        ts = dt.datetime.now(dt.timezone.utc)

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    else:
        ts = ts.astimezone(dt.timezone.utc)
    return ts


def _tf_to_timedelta(tf: str) -> Optional[dt.timedelta]:
    """
    Convert a timeframe string like '1m', '5m', '15m', '1h', '1d' or plain
    digits like '5', '15', '60' into a timedelta. If we can't parse it,
    return None and the caller can fall back to existing behavior.
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



# ---------------------------------------------------------------------------
# VP enrichment (prop-desk style context ranker)
# ---------------------------------------------------------------------------

TF_ORDER = ["1m", "3m", "5m", "15m", "1h", "1d", "1w"]
TF_WEIGHT = {"1m": 0.20, "3m": 0.25, "5m": 0.30, "15m": 0.35, "1h": 0.45, "1d": 0.60, "1w": 0.75}

def _tf_higher_or_equal(current_tf: str) -> List[str]:
    try:
        i = TF_ORDER.index(current_tf)
    except ValueError:
        return []
    return TF_ORDER[i:]

def _tf_higher(current_tf: str) -> List[str]:
    try:
        i = TF_ORDER.index(current_tf)
    except ValueError:
        return []
    return TF_ORDER[i+1:]

def _range_overlaps(a_low: float, a_high: float, b_low: float, b_high: float) -> bool:
    lo1, hi1 = (a_low, a_high) if a_low <= a_high else (a_high, a_low)
    lo2, hi2 = (b_low, b_high) if b_low <= b_high else (b_high, b_low)
    return not (hi1 < lo2 or hi2 < lo1)

def _level_in_range(level: float, low: float, high: float, pad: float = 0.0) -> bool:
    lo, hi = (low, high) if low <= high else (high, low)
    return (lo - pad) <= level <= (hi + pad)

def _trend_state(snapshot: Optional[Dict[str, Any]]) -> str:
    try:
        st = (snapshot or {}).get("trend", {}).get("state") or "unknown"
        return str(st)
    except Exception:
        return "unknown"

def _regime_state(snapshot: Optional[Dict[str, Any]]) -> str:
    # Simple proxy: if trend is range -> balance; else expansion.
    st = _trend_state(snapshot)
    if st == "range":
        return "balance"
    if st in ("bull", "bear"):
        return "expansion"
    return "unknown"

def _iter_active_fvgs(snapshot: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fvgs = (snapshot or {}).get("fvgs") or []
    out = []
    for f in fvgs:
        try:
            if f.get("filled") is True:
                continue
            # If trade_score exists and is 0, skip (fully mitigated or invalidated)
            if "trade_score" in f and float(f.get("trade_score") or 0.0) <= 0.0:
                continue
            out.append(f)
        except Exception:
            continue
    return out

def _iter_liq_levels(snapshot: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    liq = (snapshot or {}).get("liquidity") or {}
    levels = liq.get("levels") or []
    return [lv for lv in levels if isinstance(lv, dict) and "price" in lv]

def _enrich_vp_for_tf(
    *,
    current_tf: str,
    current_snapshot: Dict[str, Any],
    snapshots_by_tf: Dict[str, Dict[str, Any]],
) -> None:
    vp = (current_snapshot or {}).get("volume_profile") or {}
    profiles = vp.get("profiles") or {}
    if not isinstance(profiles, dict):
        return

    cur_trend = _trend_state(current_snapshot)
    cur_regime = _regime_state(current_snapshot)

    # Small helper to compute context multipliers + tags
    def _context_for_node(node: Dict[str, Any], *, node_kind: str) -> Tuple[float, List[str]]:
        low = float(node.get("low", 0.0))
        high = float(node.get("high", 0.0))
        tags: List[str] = []
        mult = 1.0

        # Regime preference (cheap but useful)
        if cur_regime == "balance":
            if node_kind == "hvn":
                mult *= 1.08
                tags.append("regime_balance_hvn")
            else:
                mult *= 0.92
                tags.append("regime_balance_lvn_penalty")
        elif cur_regime == "expansion":
            if node_kind == "lvn":
                mult *= 1.08
                tags.append("regime_expansion_lvn")
            else:
                mult *= 0.93
                tags.append("regime_expansion_hvn_penalty")

        # Multi-TF confluence: trend + FVG + liquidity
        for htf in _tf_higher_or_equal(current_tf):
            snap = snapshots_by_tf.get(htf)
            if not snap:
                continue

            w = TF_WEIGHT.get(htf, 0.35)

            # Trend alignment
            htf_trend = _trend_state(snap)
            if htf_trend in ("bull", "bear") and cur_trend in ("bull", "bear"):
                if htf_trend == cur_trend:
                    mult *= (1.0 + 0.10 * w)   # up to ~+7.5% for 1w
                    tags.append(f"trend_aligned_{htf}")
                else:
                    mult *= (1.0 - 0.18 * w)   # up to ~-13.5% for 1w
                    tags.append(f"trend_counter_{htf}")
            elif htf_trend == "range":
                # weekly range especially: penalize LVN breakouts (chop risk)
                if htf == "1w" and node_kind == "lvn":
                    mult *= 0.90
                    tags.append("weekly_balance_lvn_penalty")

            # FVG overlap
            fvgs = _iter_active_fvgs(snap)
            for f in fvgs:
                try:
                    fl = float(f.get("low"))
                    fh = float(f.get("high"))
                    if _range_overlaps(low, high, fl, fh):
                        # HTF overlap is more meaningful; weight it gently
                        mult *= (1.0 + 0.12 * w)
                        tags.append(f"fvg_overlap_{htf}")
                        break
                except Exception:
                    continue

            # Liquidity overlap (price level inside node)
            for lv in _iter_liq_levels(snap):
                try:
                    p = float(lv.get("price"))
                    if _level_in_range(p, low, high, pad=0.0):
                        # intact levels are better than broken levels
                        st = str(lv.get("state") or "")
                        bump = 0.08 if st == "intact" else 0.04
                        mult *= (1.0 + bump * w)
                        tags.append(f"liq_overlap_{htf}")
                        break
                except Exception:
                    continue

        # Clamp to keep it sane
        mult = max(0.60, min(1.80, mult))
        return mult, tags

    # Apply enrichment per profile, and re-rank by final_score
    for profile_name, p in profiles.items():
        if not isinstance(p, dict):
            continue

        for node_kind, key in (("hvn", "hvn_ranges"), ("lvn", "lvn_ranges")):
            nodes = p.get(key) or []
            if not isinstance(nodes, list) or not nodes:
                continue

            for nd in nodes:
                if not isinstance(nd, dict):
                    continue
                base = float(nd.get("base_score") or 0.0)
                cm, tags = _context_for_node(nd, node_kind=node_kind)
                nd["context_mult"] = float(cm)
                nd["final_score"] = float(base * cm)
                nd["tags"] = tags

            # re-rank by final_score
            nodes.sort(key=lambda x: float((x or {}).get("final_score") or 0.0), reverse=True)
            for r, nd in enumerate(nodes, start=1):
                if isinstance(nd, dict):
                    nd["rank"] = r
            p[key] = nodes

        # profile-level meta
        p.setdefault("meta", {})
        if isinstance(p["meta"], dict):
            p["meta"]["rank_basis"] = "final_score"
            p["meta"]["context_algo"] = "multi_tf_confluence_v1"

    current_snapshot["volume_profile"] = vp




# ---------------------------------------------------------------------------
# IndicatorBot
# ---------------------------------------------------------------------------

class IndicatorBot:
    """
    Reads enriched candles from CandleEngine and keeps spot_tf indicators fresh
    for all SUPPORTED_TFS.

    v1 responsibilities:
      - For each (symbol, timeframe):
          * detect if there's a new last candle (ts changed)
          * compute:
              - trend (EMA50/EMA200 + distances + state)
              - swings (HH/HL/LH/LL pivots)
              - fvgs (simple 3-candle FVGs)
              - liquidity (levels from swings)
              - volume_profile (simple price/volume bins + POC)
              - extras (ATR, vol regime)
              - structure_state (trend + swings summary)
          * patch those into spot_tf via Supabase REST.
    """

    def __init__(
        self,
        engine: CandleEngine,
        timeframes: Optional[List[str]] = None,
        # NOTE: sim_worker passes sim_mode=True. We accept it for compatibility
        # but this simulation IndicatorBot performs NO DB writes regardless.
        sim_mode: bool = True,
    ):
        self.engine = engine
        self.timeframes = timeframes or list(SUPPORTED_TFS)
        # last_processed_ts[symbol][tf] -> datetime of last processed candle
        self.last_processed_ts: Dict[str, Dict[str, dt.datetime]] = {}
        # last_snapshots[symbol][tf] -> most recent snapshot dict (for multi-TF VP context)
        self.last_snapshots: Dict[str, Dict[str, Dict[str, Any]]] = {}

        # Events cache (NEW): previous spot_events state per symbol/tf
        self.last_events_state: Dict[str, Dict[str, Dict[str, Any]]] = {}

        # FVG previous snapshot cache for transition detection (NEW)
        self.last_fvgs_cache: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

        # Liquidity pool cache per symbol (NEW)
        self.liq_pool_cache: Dict[str, Dict[str, Any]] = {}

        # ---------------- SIM DIAGNOSTICS (no DB) ----------------
        self._bootstrap_calls: int = 0
        self._on_candle_total: int = 0
        self._on_candle_by_tf: Dict[str, int] = {}
        self._calc1_calls_by_tf: Dict[str, int] = {}
        self._calc2_calls_by_tf: Dict[str, int] = {}
        self._spot_event_calls_by_tf: Dict[str, int] = {}
        self._liq_builder_calls_total: int = 0
        self._liq_builder_calls_by_tf: Dict[str, int] = {}
        self._last_spot_event_payload: Optional[Dict[str, Any]] = None
        self._first_10_received: List[Dict[str, Any]] = []
        self._last_asof: Dict[Tuple[str, str], str] = {}
        self._last_day_et_by_symbol: Dict[str, str] = {}
        self._diag_cycle_count: int = 0
        self._event_counter_log_every: int = max(0, int(os.getenv("EVENT_COUNTER_LOG_EVERY", "1") or "1"))
        self._perf_stage_totals: Dict[str, float] = {}
        self._perf_stage_counts: Dict[str, int] = {}
        self._perf_stage_totals_by_tf: Dict[str, Dict[str, float]] = {}
        self._perf_stage_counts_by_tf: Dict[str, Dict[str, int]] = {}
        self._perf_end_logged: bool = False

        # Indicator candle windowing mode:
        # - "compound": use full available history (default)
        # - "fixed": use rolling window sizes per timeframe
        self._candle_window_mode: str = (os.getenv("INDICATOR_CANDLE_WINDOW_MODE") or "compound").strip().lower()
        if self._candle_window_mode not in ("compound", "fixed"):
            self._candle_window_mode = "compound"
        self._fixed_window_default: int = max(0, int(os.getenv("INDICATOR_FIXED_CANDLES", "0") or "0"))
        self._fixed_window_by_tf: Dict[str, int] = {}
        for tf in SUPPORTED_TFS:
            env_name = f"INDICATOR_FIXED_CANDLES_{tf.upper()}"
            raw = os.getenv(env_name)
            if raw is None or raw == "":
                self._fixed_window_by_tf[tf] = self._fixed_window_default
                continue
            try:
                self._fixed_window_by_tf[tf] = max(0, int(raw))
            except Exception:
                self._fixed_window_by_tf[tf] = self._fixed_window_default
    # ------------------------------------------------------------------ #
    # Simulation entrypoints (sim_worker depends on these)
    # ------------------------------------------------------------------ #

    def _window_candles(self, timeframe: str, candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if self._candle_window_mode != "fixed":
            return candles
        n = int(self._fixed_window_by_tf.get(timeframe, self._fixed_window_default) or 0)
        if n <= 0:
            return candles
        if len(candles) <= n:
            return candles
        return candles[-n:]

    async def bootstrap(self, symbol: str, seed: Dict[str, Any]) -> None:
        """
        Simulation bootstrap.
        sim_worker provides `seed` as { "1m":[...], "3m":[...], ... }.
        We DO NOT write to DB. We just preload engine + caches so subsequent
        on_candle() calls behave like live updates.
        """
        self._bootstrap_calls += 1
        sym = (symbol or "").upper()
        if not sym or not isinstance(seed, dict):
            return

        # Ensure engine has seed candles (CandleEngine may already have done this,
        # but we keep it explicit for determinism).
        self.engine.candles.setdefault(sym, {})
        for tf, arr in seed.items():
            if not isinstance(arr, list):
                continue
            self.engine.candles[sym][tf] = list(arr)

        # Prime snapshots/last_processed_ts from seed, without DB.
        self.last_processed_ts.setdefault(sym, {})
        self.last_snapshots.setdefault(sym, {})
        self.last_events_state.setdefault(sym, {})
        self.last_fvgs_cache.setdefault(sym, {})

        # Compute an initial snapshot per TF if we have enough candles.
        for tf in self.timeframes:
            candles = self.engine.candles.get(sym, {}).get(tf) or []
            if len(candles) < 10:
                continue
            closed_candles = self._window_candles(tf, candles)  # seed is assumed closed/ordered
            last_candle = closed_candles[-1]
            ts_dt = _ensure_dt(last_candle.get("ts"))

            # Session candles (advanced extras)
            session_date = last_candle.get("date_et")
            if session_date:
                session_candles = [c for c in closed_candles if c.get("date_et") == session_date]
            else:
                session_candles = closed_candles

            snapshot = compute_all_indicators(closed_candles)
            self._calc1_calls_by_tf[tf] = self._calc1_calls_by_tf.get(tf, 0) + 1

            advanced = compute_advanced_extras(
                candles=closed_candles,
                base_snapshot=snapshot,
                htf_snapshot=None,
                session_candles=session_candles,
            )
            self._calc2_calls_by_tf[tf] = self._calc2_calls_by_tf.get(tf, 0) + 1

            # Attach extras_advanced to the cached snapshot so downstream consumers
            # see the same "row shape" we would have written to spot_tf.
            snapshot["extras_advanced"] = advanced

            self.last_snapshots[sym][tf] = snapshot
            self.last_processed_ts[sym][tf] = ts_dt

        # One-time multi-TF VP enrichment using the seeded snapshot map.
        snap_map: Dict[str, Dict[str, Any]] = dict(self.last_snapshots.get(sym, {}))
        for tf, snap in snap_map.items():
            try:
                _enrich_vp_for_tf(current_tf=tf, current_snapshot=snap, snapshots_by_tf=snap_map)
            except Exception:
                pass

        # ------------------------------------------------------------
        # Build liquidity pool once from seed snapshots (like live bot)
        # ------------------------------------------------------------
        try:
            spot_rows: List[Dict[str, Any]] = []
            snap_by_tf = self.last_snapshots.get(sym, {}) or {}
            for tf in self.timeframes:
                snap = snap_by_tf.get(tf)
                if not isinstance(snap, dict):
                    continue
                spot_rows.append(
                    {
                        "symbol": sym,
                        "timeframe": tf,
                        "asof": snap.get("asof"),
                        "trend": snap.get("trend"),
                        "pivots": snap.get("pivots"),
                        "swings": snap.get("swings"),
                        "structural": snap.get("structural"),
                        "fvgs": snap.get("fvgs"),
                        "liquidity": snap.get("liquidity"),
                        "volume_profile": snap.get("volume_profile"),
                        "extras": snap.get("extras"),
                        "extras_advanced": snap.get("extras_advanced"),
                        "structure_state": snap.get("structure_state"),
                        "strategies": snap.get("strategies"),
                    }
                )

            if spot_rows:
                self._liq_builder_calls_total += 1
                self._liq_builder_calls_by_tf["bootstrap"] = self._liq_builder_calls_by_tf.get("bootstrap", 0) + 1

                t0 = time.perf_counter()
                pool = build_liquidity_pool(symbol=sym, spot_tf_rows=spot_rows, candle_engine=self.engine)
                self._record_perf("liquidity_build", time.perf_counter() - t0, tf="bootstrap")
                if isinstance(pool, dict):
                    self.liq_pool_cache[sym] = {
                        "symbol": sym,
                        "asof": pool.get("asof"),
                        "pool_version": pool.get("pool_version") or "liq_pool_v1",
                        "levels": pool.get("levels") or [],
                        "stats": pool.get("stats") or {},
                        "tol_price": pool.get("tol_price"),
                        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    }
        except Exception as e:
            pass
            # print(f"[INDICATOR_BOT][DIAG] bootstrap liquidity pool build failed: {e}")

    async def on_candle(self, symbol: str, timeframe: str, candle: Dict[str, Any]) -> None:
        """
        Simulation feed entrypoint called by sim_worker for each emitted candle.
        We record diagnostics, then run the same update pipeline (no DB writes).
        """
        sym = (symbol or "").upper()
        tf = str(timeframe or "")
        if not sym or not tf or not isinstance(candle, dict):
            return

        # Deduplicate by asof/ts per (symbol, tf)
        asof = candle.get("ts") or candle.get("asof")
        key = (sym, tf)
        if asof is not None:
            s_asof = str(asof)
            if self._last_asof.get(key) == s_asof:
                return
            self._last_asof[key] = s_asof

        self._on_candle_total += 1
        self._on_candle_by_tf[tf] = self._on_candle_by_tf.get(tf, 0) + 1

        # Keep first 10 received candles (exactly what we got)
        if len(self._first_10_received) < 10:
            self._first_10_received.append(
                {
                    "tf": tf,
                    "ts": candle.get("ts"),
                    "ts_et": candle.get("ts_et"),
                    "open": candle.get("open"),
                    "high": candle.get("high"),
                    "low": candle.get("low"),
                    "close": candle.get("close"),
                    "volume": candle.get("volume"),
                    "session": candle.get("session"),
                }
            )
            if len(self._first_10_received) == 10:
                # print("[INDICATOR_BOT][DIAG] First 10 candles received by on_candle:")
                for i, row in enumerate(self._first_10_received, start=1):
                    pass
                    # print(f"[INDICATOR_BOT][DIAG] #{i}: {row}")

        # Track day_et for summary
        day_et = candle.get("date_et")
        if day_et:
            self._last_day_et_by_symbol[sym] = str(day_et)

        # Run the standard update pipeline
        await self._update_all_symbols()

    def _record_perf(self, stage: str, seconds: float, tf: Optional[str] = None) -> None:
        if not stage:
            return
        safe_seconds = float(max(0.0, seconds))
        self._perf_stage_totals[stage] = self._perf_stage_totals.get(stage, 0.0) + safe_seconds
        self._perf_stage_counts[stage] = self._perf_stage_counts.get(stage, 0) + 1
        if tf:
            tfx = str(tf)
            self._perf_stage_totals_by_tf.setdefault(tfx, {})
            self._perf_stage_counts_by_tf.setdefault(tfx, {})
            self._perf_stage_totals_by_tf[tfx][stage] = self._perf_stage_totals_by_tf[tfx].get(stage, 0.0) + safe_seconds
            self._perf_stage_counts_by_tf[tfx][stage] = self._perf_stage_counts_by_tf[tfx].get(stage, 0) + 1

    def _print_perf_summary_once(self, symbol: str) -> None:
        if self._perf_end_logged:
            return
        self._perf_end_logged = True
        sym = (symbol or "").upper()
        print(f"[INDICATOR_BOT][PERF] Final performance summary for {sym}")
        if not self._perf_stage_totals:
            print("[INDICATOR_BOT][PERF] No timing samples collected.")
            return

        for stage in sorted(self._perf_stage_totals.keys()):
            total_s = self._perf_stage_totals.get(stage, 0.0)
            count = self._perf_stage_counts.get(stage, 0)
            avg_ms = (total_s / count * 1000.0) if count > 0 else 0.0
            print(f"[INDICATOR_BOT][PERF] stage={stage} count={count} total_s={total_s:.6f} avg_ms={avg_ms:.3f}")

        for tf in sorted(self._perf_stage_totals_by_tf.keys(), key=lambda x: (len(x), x)):
            stage_totals = self._perf_stage_totals_by_tf.get(tf, {})
            stage_counts = self._perf_stage_counts_by_tf.get(tf, {})
            for stage in sorted(stage_totals.keys()):
                total_s = stage_totals.get(stage, 0.0)
                count = stage_counts.get(stage, 0)
                avg_ms = (total_s / count * 1000.0) if count > 0 else 0.0
                print(
                    f"[INDICATOR_BOT][PERF][TF={tf}] stage={stage} count={count} total_s={total_s:.6f} avg_ms={avg_ms:.3f}"
                )

    def print_event_summary(self) -> None:
        """
        sim_worker expects this. Print a compact diagnostic summary.
        """
        for sym in sorted(self._last_day_et_by_symbol.keys() or []):
            day_et = self._last_day_et_by_symbol.get(sym)
            # print(f"[INDICATOR_BOT][DIAG] Summary for {sym} day_et={day_et}")
        # print(f"[INDICATOR_BOT][DIAG] on_candle_total={self._on_candle_total} on_candle_by_tf={self._on_candle_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] calc1_calls_by_tf={self._calc1_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] calc2_calls_by_tf={self._calc2_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] spot_event_calls_by_tf={self._spot_event_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] liquidity_builder_calls_total={self._liq_builder_calls_total}")
        # print(f"[INDICATOR_BOT][DIAG] liquidity_builder_calls_by_tf={self._liq_builder_calls_by_tf}")
        # Spot-event per-event trigger counters (printed by spot_event module)
        try:
            from spot_event import print_spot_event_counters  # local import to avoid cycles
            print_spot_event_counters()
        except Exception as e:
            pass
            # print(f"[INDICATOR_BOT][DIAG] spot_event counters unavailable: {e}")

    def dump_diag_counts(self, symbol: str) -> None:
        """
        sim_worker expects this.
        """
        sym = (symbol or "").upper()
        day_et = self._last_day_et_by_symbol.get(sym)
        # print(f"[INDICATOR_BOT][DIAG] Summary for {sym} day_et={day_et}")
        # print(f"[INDICATOR_BOT][DIAG] on_candle_total={self._on_candle_total} on_candle_by_tf={self._on_candle_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] calc1_calls_by_tf={self._calc1_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] calc2_calls_by_tf={self._calc2_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] spot_event_calls_by_tf={self._spot_event_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] liquidity_builder_calls_total={self._liq_builder_calls_total}")
        # print(f"[INDICATOR_BOT][DIAG] liquidity_builder_calls_by_tf={self._liq_builder_calls_by_tf}")
        # Spot-event per-event trigger counters (printed by spot_event module)
        try:
            from spot_event import print_spot_event_counters  # local import to avoid cycles
            print_spot_event_counters()
        except Exception as e:
            pass
            # print(f"[INDICATOR_BOT][DIAG] spot_event counters unavailable: {e}")
        self._print_perf_summary_once(symbol)



    

    # ------------------------------------------------------------------ #
    # Public loop
    # ------------------------------------------------------------------ #

    async def run_loop(self, interval_seconds: int = 60) -> None:
        """
        Periodically scan all symbols/timeframes and update indicators
        whenever a new candle has appeared.
        """
        # print(f"[INDICATORS] Starting indicator bot (interval={interval_seconds}s)")

        while True:
            try:
                await self._update_all_symbols()
                self._maybe_log_event_counters()
            except Exception as e:
                pass
                # print(f"[INDICATORS] Exception in run_loop: {e}")
            await asyncio.sleep(interval_seconds)

    def _maybe_log_event_counters(self) -> None:
        """
        Runtime printing for spot-event counters.
        Controlled by env var EVENT_COUNTER_LOG_EVERY:
          0 = disabled
          1 = print every cycle
          N = print every N cycles
        """
        every = self._event_counter_log_every
        if every <= 0:
            return

        self._diag_cycle_count += 1
        if (self._diag_cycle_count % every) != 0:
            return

        # print(f"[INDICATOR_BOT][DIAG] cycle={self._diag_cycle_count}")
        # print(f"[INDICATOR_BOT][DIAG] on_candle_total={self._on_candle_total} on_candle_by_tf={self._on_candle_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] calc1_calls_by_tf={self._calc1_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] calc2_calls_by_tf={self._calc2_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] spot_event_calls_by_tf={self._spot_event_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] liquidity_builder_calls_total={self._liq_builder_calls_total}")
        # print(f"[INDICATOR_BOT][DIAG] liquidity_builder_calls_by_tf={self._liq_builder_calls_by_tf}")

        try:
            from spot_event import print_spot_event_counters  # local import to avoid cycles

            print_spot_event_counters()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Core update logic
    # ------------------------------------------------------------------ #

    
    async def _update_all_symbols(self) -> None:
        symbols = getattr(self.engine, "symbols", [])
        if not symbols:
            # print("[INDICATORS] No symbols in engine yet, skipping cycle")
            return
    
        for sym in symbols:
            sym_upper = sym.upper()
            symbol_candles = self.engine.candles.get(sym_upper, {})

            # Simulation: DO NOT fetch liquidity pool row from DB.
            # We build/update liquidity pool from cached spot_rows below.
    
            # Collect all TF updates first so we can do multi-TF VP enrichment
            pending: Dict[str, Dict[str, Any]] = {}
    
            for tf in self.timeframes:
                tf_prep_start = time.perf_counter()
                candles = symbol_candles.get(tf)
                if not candles or len(candles) < 10:
                    continue
    
                # --------------------------------------------------
                # CLOSED-CANDLE FILTER
                # --------------------------------------------------
                tf_delta = _tf_to_timedelta(tf)
                now_utc = dt.datetime.now(dt.timezone.utc)
    
                closed_candles = candles
                if tf_delta is not None:
                    raw_last = candles[-1]
                    raw_last_ts = _ensure_dt(raw_last.get("ts"))
                    if now_utc < (raw_last_ts + tf_delta):
                        closed_candles = candles[:-1]
    
                closed_candles = self._window_candles(tf, closed_candles)
                if not closed_candles or len(closed_candles) < 10:
                    continue
    
                last_candle = closed_candles[-1]
                ts_dt = _ensure_dt(last_candle.get("ts"))
    
                prev_ts = self.last_processed_ts.get(sym_upper, {}).get(tf)
                # Simulation: no DB; no backfill checks against spot_tf.
                needs_backfill = False
    
                if (not needs_backfill) and (prev_ts is not None) and (ts_dt <= prev_ts):
                    continue
    
                # Session candles (for advanced extras)
                session_date = last_candle.get("date_et")
                if session_date:
                    session_candles = [c for c in closed_candles if c.get("date_et") == session_date]
                else:
                    session_candles = closed_candles
    
                # Base indicators (single pass)
                t0 = time.perf_counter()
                snapshot = compute_all_indicators(closed_candles)
                self._record_perf("calc1", time.perf_counter() - t0, tf=tf)
                self._calc1_calls_by_tf[tf] = self._calc1_calls_by_tf.get(tf, 0) + 1
    
                # Advanced extras (needs base_snapshot)
                t0 = time.perf_counter()
                advanced = compute_advanced_extras(
                    candles=closed_candles,
                    base_snapshot=snapshot,
                    htf_snapshot=None,
                    session_candles=session_candles,
                )
                self._record_perf("calc2", time.perf_counter() - t0, tf=tf)
                self._calc2_calls_by_tf[tf] = self._calc2_calls_by_tf.get(tf, 0) + 1
    
                pending[tf] = {
                    "closed_candles": closed_candles,
                    "session_candles": session_candles,
                    "snapshot": snapshot,
                    "extras_advanced": advanced,
                    "last_candle": last_candle,
                    "ts_dt": ts_dt,
                }
                self._record_perf("tf_pipeline_prep", time.perf_counter() - tf_prep_start, tf=tf)
    
            if not pending:
                continue
    
            # Build multi-TF snapshot map (use cached snapshots for TFs that didn't update this cycle)
            snap_map: Dict[str, Dict[str, Any]] = {}
            snap_map.update(self.last_snapshots.get(sym_upper, {}))
            for tf, pack in pending.items():
                snap_map[tf] = pack["snapshot"]
    
            # Enrich VP for each pending TF with multi-TF context
            for tf, pack in pending.items():
                try:
                    t0 = time.perf_counter()
                    _enrich_vp_for_tf(
                        current_tf=tf,
                        current_snapshot=pack["snapshot"],
                        snapshots_by_tf=snap_map,
                    )
                    self._record_perf("vp_enrich", time.perf_counter() - t0, tf=tf)
                except Exception as e:
                    pass
                    # print(f"[VP][ENRICH] Failed for {sym_upper} {tf}: {e}")
    
            # Now write to DB (and update caches)
            spot_last_candle = ((pending.get("1m") or {}).get("last_candle") if isinstance(pending, dict) else None)
            for tf, pack in pending.items():
                snapshot = pack["snapshot"]
                last_candle = pack["last_candle"]
                ts_dt = pack["ts_dt"]
    
                trend = snapshot["trend"]
                pivots = snapshot["pivots"]
                swings = snapshot["swings"]
                structural = snapshot["structural"]
                fvgs = snapshot["fvgs"]
                liquidity = snapshot["liquidity"]
                volume_profile = snapshot["volume_profile"]
                extras = snapshot["extras"]
                structure_state = snapshot["structure_state"]
                extras_advanced = pack["extras_advanced"]
    
                cluster = last_candle.get("cluster") or {}

                t0 = time.perf_counter()
                strategies = evaluate_strategies(
                    symbol=sym_upper,
                    timeframe=tf,
                    candles=pack["closed_candles"],
                    swings=swings,
                    fvgs=fvgs,
                    liquidity=liquidity,
                    trend=trend,
                    cluster=cluster,
                    volume_profile=volume_profile,
                    htf_swings=None,
                    structure_states_by_tf={
                        k: (v or {}).get("structure_state")
                        for k, v in (snap_map or {}).items()
                        if isinstance(v, dict)
                    },
                    spot_last_candle=spot_last_candle,
                )
                self._record_perf("strategy_eval", time.perf_counter() - t0, tf=tf)

                # ---------------- EVENTS (NEW) ----------------
                prev_state = self.last_events_state.get(sym_upper, {}).get(tf) or {}
                prev_latest = prev_state.get("events_latest")
                prev_active = prev_state.get("events_active")
                prev_recent = prev_state.get("events_recent")

                fvgs_prev = self.last_fvgs_cache.get(sym_upper, {}).get(tf)
                liq_row = self.liq_pool_cache.get(sym_upper)

                # Build reference lists from YOUR stored shapes:
                # swings: {"asof":..., "swings":[{ts,type,price,state,...}, ...]}
                swing_items = (swings or {}).get("swings") or []
                swing_highs = [
                    {"ts": s.get("ts"), "price": s.get("price")}
                    for s in swing_items
                    if s.get("type") == "swing_high" # and s.get("state") == "active"
                ]
                swing_lows = [
                    {"ts": s.get("ts"), "price": s.get("price")}
                    for s in swing_items
                    if s.get("type") == "swing_low" # and s.get("state") == "active"
                ]

                # structural: {"asof":..., "points":[{ts,type,label,price,...}, ...]}
                structural_points = (structural or {}).get("points") or []
                structural_highs = [
                    {"ts": p.get("ts"), "price": p.get("price")}
                    for p in structural_points
                    if p.get("type") == "swing_high" and p.get("label") in ("HH", "LH")
                ]
                structural_lows = [
                    {"ts": p.get("ts"), "price": p.get("price")}
                    for p in structural_points
                    if p.get("type") == "swing_low" and p.get("label") in ("LL", "HL")
                ]

                ev_ctx = SpotEventContext(
                    symbol=sym_upper,
                    timeframe=tf,
                    last_candle=last_candle,
                    liquidity_pool_row=liq_row,
                    structural_highs=structural_highs,
                    structural_lows=structural_lows,
                    swing_highs=swing_highs,
                    swing_lows=swing_lows,
                    structure_state=structure_state or "",
                    fvgs_now=fvgs,
                    fvgs_prev=fvgs_prev,
                    prev_events_latest=prev_latest,
                    prev_events_active=prev_active,
                    prev_events_recent=prev_recent,
                )

                # Capture ONLY the last payload sent into spot_event (print once at end)
                try:
                    self._last_spot_event_payload = vars(ev_ctx)
                except Exception:
                    # Fallback: keep a minimal safe representation (still "what we sent")
                    self._last_spot_event_payload = {"repr": repr(ev_ctx)}

                t0 = time.perf_counter()
                ev_out = compute_spot_events(ev_ctx)
                self._record_perf("spot_event", time.perf_counter() - t0, tf=tf)
                self._spot_event_calls_by_tf[tf] = self._spot_event_calls_by_tf.get(tf, 0) + 1

                # Attach extras_advanced + strategies to snapshot so cached "row shape"
                # is what zone/liquidity builders expect (no DB writes).
                try:
                    snapshot["extras_advanced"] = extras_advanced
                except Exception:
                    pass
                try:
                    snapshot["strategies"] = strategies
                except Exception:
                    pass
    
                # Simulation: NO DB writes (spot_tf).

                # Simulation: NO DB writes (spot_events).

                # Update caches (NEW)
                self.last_events_state.setdefault(sym_upper, {})[tf] = {
                    "events_latest": ev_out["events_latest"],
                    "events_active": ev_out["events_active"],
                    "events_recent": ev_out["events_recent"],
                }
                self.last_fvgs_cache.setdefault(sym_upper, {})[tf] = list(fvgs or [])
    
                # Cache for next cycle's multi-TF enrichment
                self.last_snapshots.setdefault(sym_upper, {})[tf] = snapshot
                self.last_processed_ts.setdefault(sym_upper, {})[tf] = ts_dt
    
                # (log removed) was too noisy during simulation troubleshooting
                # print(
                #     f"[INDICATORS] Updated {sym_upper} {tf} "
                #     f"(ts={ts_dt.isoformat()}, trend={trend.get('state')})"
                # )
    
            # ---------------- FVG POOL (all TFs -> one symbol-level latest pool) ----------------
            try:
                # Use in-memory cache as the source of spot_tf_rows (DB is write-only/verification).
                spot_rows: List[Dict[str, Any]] = []
                snap_by_tf = self.last_snapshots.get(sym_upper, {}) or {}
                for tf in self.timeframes:
                    snap = snap_by_tf.get(tf)
                    if not isinstance(snap, dict):
                        continue

                    # If this TF updated in the current cycle, use the freshly computed extras_advanced.
                    extras_adv = None
                    if tf in pending:
                        extras_adv = pending[tf].get("extras_advanced")
                    else:
                        extras_adv = snap.get("extras_advanced")

                    spot_rows.append(
                        {
                            "symbol": sym_upper,
                            "timeframe": tf,
                            "asof": snap.get("asof"),
                            "trend": snap.get("trend"),
                            "pivots": snap.get("pivots"),
                            "swings": snap.get("swings"),
                            "structural": snap.get("structural"),
                            "fvgs": snap.get("fvgs"),
                            "liquidity": snap.get("liquidity"),
                            "volume_profile": snap.get("volume_profile"),
                            "extras": snap.get("extras"),
                            "extras_advanced": extras_adv,
                            "structure_state": snap.get("structure_state"),
                            # Strategies are written to DB, but not required for liquidity/zone building;
                            # include if present in cache.
                            "strategies": snap.get("strategies"),
                        }
                    )

                # Count how many times liquidity builder is called
                self._liq_builder_calls_total += 1
                # Attribute this pool rebuild to TFs that updated in this cycle
                for _tf in pending.keys():
                    self._liq_builder_calls_by_tf[_tf] = self._liq_builder_calls_by_tf.get(_tf, 0) + 1
                t0 = time.perf_counter()
                pool = build_liquidity_pool(
                    symbol=sym_upper,
                    spot_tf_rows=spot_rows,
                    candle_engine=self.engine,
                )
                self._record_perf("liquidity_build", time.perf_counter() - t0, tf="live")
                # Simulation: NO DB writes (spot_liquidity_pool).

                # Refresh liquidity pool cache from computed pool (NEW)
                self.liq_pool_cache[sym_upper] = {
                    "symbol": sym_upper,
                    "asof": pool.get("asof"),
                    "pool_version": pool.get("pool_version") or "liq_pool_v1",
                    "levels": pool.get("levels") or [],
                    "stats": pool.get("stats") or {},
                    "tol_price": pool.get("tol_price"),
                    "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                }

                fvg_rows: List[Dict[str, Any]] = []
                current_price = None
                snap_1m = snap_by_tf.get("1m") if isinstance(snap_by_tf, dict) else None
                if isinstance(snap_1m, dict):
                    try:
                        current_price = float((snap_1m.get("last_candle") or {}).get("close"))
                    except Exception:
                        current_price = None
                for tf in self.timeframes:
                    snap = snap_by_tf.get(tf)
                    if not isinstance(snap, dict):
                        continue
                    fvg_rows.append(
                        {
                            "timeframe": tf,
                            "fvgs": snap.get("fvgs") or [],
                            "current_price": current_price,
                        }
                    )

                fvg_pool = build_symbol_fvg_pool(
                    symbol=sym_upper,
                    fvg_rows=fvg_rows,
                    current_price=current_price,
                )
                await upsert_symbol_fvg_pool(
                    fvg_pool=fvg_pool,
                    table="fvg_pool",
                )

                meta = fvg_pool.get("meta") or {}
                # print(
                    # f"[FVG_POOL] Updated {sym_upper} "
                    # f"raw={meta.get('raw_fvg_count', 0)} "
                    # f"eligible={meta.get('eligible_fvg_count', 0)} "
                    # f"bull_pools={meta.get('bullish_pool_count', 0)} "
                    # f"bear_pools={meta.get('bearish_pool_count', 0)}"
                # )
                # print(f"[FVG_POOL][FINAL] {json.dumps(fvg_pool, indent=2, default=str)}")

            except Exception as e:
                pass
                # print(f"[FVG_POOL] Failed for {sym_upper}: {e}")
