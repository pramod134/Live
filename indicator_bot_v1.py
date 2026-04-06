import math
import os
import asyncio
import datetime as dt
import json
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo


from candle_engine import CandleEngine, SUPPORTED_TFS

from indicator_calc1 import compute_all_indicators

from indicator_calc2 import compute_advanced_extras



from strategies import evaluate_strategies
from liquidity_pool_builder import build_liquidity_pool

# Events engine (NEW)
from spot_event import SpotEventContext, compute_spot_events






# Logging disabled outside sim_worker per repository policy.
def print(*args, **kwargs):  # type: ignore[override]
    return None

EASTERN = ZoneInfo("America/New_York")
ENABLE_SPOT_EVENTS = str(os.getenv("SPOT_EVENTS_ENABLED", "0")).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


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
    Live-mode DB write helper (disabled in simulation).
    In simulation we keep everything in-memory; this is a no-op.
    """
    return


async def needs_backfill_structure_state(symbol: str, timeframe: str) -> bool:
    """
    Simulation mode: no DB state to backfill; always False.
    """
    return False


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

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
    Simulation mode:
      - Runner drives CandleEngine.step()
      - Runner calls IndicatorBot.bootstrap(seed_data) once
      - Runner calls IndicatorBot.process(events) each step

    No polling. No DB writes. Everything stored in caches.
    """

    def __init__(
        self,
        engine: CandleEngine,
        timeframes: Optional[List[str]] = None,
        enable_zone_finder: bool = False,
        enable_liquidity_pool: bool = False,
        # NOTE: sim_worker passes sim_mode=True. We accept it for compatibility
        # and keep simulation behavior in-memory (no DB writes).
        sim_mode: bool = True,
    ):
        self.engine = engine
        self.timeframes = timeframes or list(SUPPORTED_TFS)
        self.enable_zone_finder = enable_zone_finder
        self.enable_liquidity_pool = enable_liquidity_pool
        self.sim_mode = bool(sim_mode)
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

        # Simulation output caches (NEW)
        # spot_tf_cache[(symbol, tf)] -> latest computed snapshot and metadata
        self.spot_tf_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
        # trades_cache[run_id] optional, runner can also own this
        self.trades_cache: Dict[str, List[Dict[str, Any]]] = {}

        # Push-mode simulation helpers
        self._last_asof: Dict[Tuple[str, str], str] = {}
        self._sim_candles: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

        # ---------------- SIM DIAGNOSTICS (no DB) ----------------
        self._bootstrap_calls: int = 0
        self._on_candle_total: int = 0
        self._on_candle_by_tf: Dict[str, int] = {}
        self._calc1_calls_by_tf: Dict[str, int] = {}
        self._calc2_calls_by_tf: Dict[str, int] = {}
        self._spot_event_calls_by_tf: Dict[str, int] = {}
        self._first_10_received: List[Dict[str, Any]] = []
        self._last_day_et_by_symbol: Dict[str, str] = {}

        # Logging guards
        self._logged_indicators_once: Dict[Tuple[str, str], bool] = {}
        self._logged_liqpool_once: Dict[Tuple[str, str], bool] = {}

        # ---------------- Event counters (final summary only) ----------------
        # Count on *new occurrences* (per-event timestamp changes), not just None->non-None.
        # This is important because events_latest slots are overwritten (value stays non-None)
        # and trackers can refresh without going back to None.
        self._last_seen_latest_ts: Dict[str, str] = {}          # events_latest[name] -> last seen event.ts
        self._last_seen_active_anchor_ts: Dict[str, str] = {}   # events_active[slot].active.anchor_ts -> last seen

        # Keep candle timestamps for each event occurrence (printed in final summary).
        # Stored as compact strings: "<tf>:<ts>".
        self._event_ts_latest: Dict[str, List[str]] = {}
        self._event_ts_active: Dict[str, List[str]] = {}

        # totals
        self._event_total_latest: Dict[str, int] = {}
        self._event_total_active: Dict[str, int] = {}

        # by timeframe
        self._event_by_tf_latest: Dict[str, Dict[str, int]] = {}
        self._event_by_tf_active: Dict[str, Dict[str, int]] = {}

        # by day (ET date)
        self._event_by_day_latest: Dict[str, Dict[str, int]] = {}
        self._event_by_day_active: Dict[str, Dict[str, int]] = {}

        # ---------------- Diagnostics (minimal, end-of-run only) ----------------
        # spot_event calls per TF
        self._diag_spot_event_calls_by_tf: Dict[str, int] = defaultdict(int)
        self._stage_timing_ms: Dict[str, Dict[str, float]] = {}
        # liquidity builder calls per TF (TF that triggered rebuild)
        self._diag_liq_builder_calls_by_tf: Dict[str, int] = defaultdict(int)

    def dump_diag_counts(self, symbol: str = "") -> None:
        """sim_worker compatibility diagnostics."""
        sym = (symbol or "").upper()
        day_et = self._last_day_et_by_symbol.get(sym) if sym else None
        if sym:
            pass
            # print(f"[INDICATOR_BOT][DIAG] Summary for {sym} day_et={day_et}")
        # print(f"[INDICATOR_BOT][DIAG] on_candle_total={self._on_candle_total} on_candle_by_tf={self._on_candle_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] calc1_calls_by_tf={self._calc1_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] calc2_calls_by_tf={self._calc2_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] spot_event_calls_by_tf={self._spot_event_calls_by_tf}")
        # print(f"[DIAG] spot_event_calls_by_tf={dict(self._diag_spot_event_calls_by_tf)}")
        # print(f"[DIAG] liq_builder_calls_by_tf={dict(self._diag_liq_builder_calls_by_tf)}")

    def _maybe_rebuild_liquidity_pool(self, *, symbol: str, trigger_tf: str) -> None:
        """Rebuild liquidity pool (if enabled) and count calls per trigger TF."""
        if not self.enable_liquidity_pool:
            return
        try:
            # Build minimal spot rows from latest snapshots
            spot_rows = []
            for tfx in self.timeframes:
                snapx = self.last_snapshots.get(symbol, {}).get(tfx)
                if not isinstance(snapx, dict):
                    continue
                spot_rows.append(
                    {
                        "symbol": symbol,
                        "timeframe": tfx,
                        "asof": snapx.get("asof"),
                        "liquidity": snapx.get("liquidity") or {},
                        "extras": snapx.get("extras") or {},
                    }
                )

            pool = build_liquidity_pool(symbol=symbol, spot_tf_rows=spot_rows, candle_engine=self.engine)
            self.liq_pool_cache[symbol] = pool
            self._diag_liq_builder_calls_by_tf[str(trigger_tf)] += 1
        except Exception as e:
            pass
            # print(f"[DIAG][LIQ_POOL] rebuild failed trigger_tf={trigger_tf} err={e}")

    def _json(self, obj: Any) -> str:
        try:
            return json.dumps(obj, default=str, ensure_ascii=False)
        except Exception:
            return str(obj)

    def _log_indicators_once(
        self,
        symbol: str,
        tf: str,
        asof: dt.datetime,
        snapshot: Dict[str, Any],
        advanced: Any,
        strategies: Any,
    ) -> None:
        """
        Log indicators only ONE time per (symbol, tf), and only first 100 chars.
        """
        key = (symbol, tf)
        if self._logged_indicators_once.get(key):
            return
        self._logged_indicators_once[key] = True

        # Keep it small: avoid dumping pivots/swings/structural arrays
        payload = {
            "trend": (snapshot or {}).get("trend"),
            "structure_state": (snapshot or {}).get("structure_state"),
            "fvgs_count": len((snapshot or {}).get("fvgs") or []),
            "swings_count": len(((snapshot or {}).get("swings") or {}).get("swings") or []),
            "strategies": strategies,
        }
        s = self._json(payload)

    def _inc_event(self, bucket: Dict[str, int], name: str) -> None:
        bucket[name] = bucket.get(name, 0) + 1

    def _inc_event_tf(self, bucket: Dict[str, Dict[str, int]], tf: str, name: str) -> None:
        d = bucket.setdefault(tf, {})
        d[name] = d.get(name, 0) + 1

    def _inc_event_day(self, bucket: Dict[str, Dict[str, int]], day_et: str, name: str) -> None:
        d = bucket.setdefault(day_et, {})
        d[name] = d.get(name, 0) + 1

    def _append_event_ts(self, bucket: Dict[str, List[str]], name: str, tf: str, ts: str) -> None:
        if not ts:
            return
        arr = bucket.setdefault(name, [])
        arr.append(f"{tf}:{ts}")

    def _track_event_counts(self, symbol: str, tf: str, asof_utc: dt.datetime, ev_out: Dict[str, Any]) -> None:
        """
        Track counts (no printing):
          - totals
          - by timeframe
          - by day (ET)
        Count occurrences by tracking per-event timestamp changes for each key.
        """
        latest = (ev_out or {}).get("events_latest") or {}
        active = (ev_out or {}).get("events_active") or {}

        # derive ET day
        try:
            asof_et = asof_utc.astimezone(ZoneInfo("America/New_York"))
            day_et = asof_et.date().isoformat()
        except Exception:
            day_et = str(asof_utc.date())

        # latest occurrences (count when the event's own ts changes)
        for name, value in latest.items():
            if value is None:
                continue
            ev_ts: Optional[str] = None
            if isinstance(value, dict):
                ev_ts = value.get("ts")
            if not ev_ts:
                # fallback: use candle asof (still useful for debugging)
                ev_ts = asof_utc.isoformat()

            prev_ts = self._last_seen_latest_ts.get(name)
            if ev_ts != prev_ts:
                self._inc_event(self._event_total_latest, name)
                self._inc_event_tf(self._event_by_tf_latest, tf, name)
                self._inc_event_day(self._event_by_day_latest, day_et, name)
                self._append_event_ts(self._event_ts_latest, name, tf, ev_ts)
                self._last_seen_latest_ts[name] = ev_ts

        # active occurrences (trackers: count when active.anchor_ts changes)
        for name, value in active.items():
            if not isinstance(value, dict):
                continue
            active_obj = value.get("active")
            if not isinstance(active_obj, dict):
                continue
            if active_obj.get("status") != "active":
                continue

            anchor_ts = active_obj.get("anchor_ts") or active_obj.get("ts")
            if not anchor_ts:
                anchor_ts = asof_utc.isoformat()

            prev_anchor_ts = self._last_seen_active_anchor_ts.get(name)
            if anchor_ts != prev_anchor_ts:
                self._inc_event(self._event_total_active, name)
                self._inc_event_tf(self._event_by_tf_active, tf, name)
                self._inc_event_day(self._event_by_day_active, day_et, name)
                self._append_event_ts(self._event_ts_active, name, tf, anchor_ts)
                self._last_seen_active_anchor_ts[name] = anchor_ts

    def print_event_summary(self) -> None:
        """Print compact simulation diagnostic summary."""
        for sym in sorted(self._last_day_et_by_symbol.keys()):
            day_et = self._last_day_et_by_symbol.get(sym)
            # print(f"[INDICATOR_BOT][DIAG] Summary for {sym} day_et={day_et}")
        # print(f"[INDICATOR_BOT][DIAG] on_candle_total={self._on_candle_total} on_candle_by_tf={self._on_candle_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] calc1_calls_by_tf={self._calc1_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] calc2_calls_by_tf={self._calc2_calls_by_tf}")
        # print(f"[INDICATOR_BOT][DIAG] spot_event_calls_by_tf={self._spot_event_calls_by_tf}")

    async def run(self) -> None:
        """
        Live runner loop. Simulation workers should use bootstrap/on_candle.
        """
        while True:
            await self.poll_once()
            await asyncio.sleep(getattr(self, "poll_seconds", 1))

    async def poll_once(self) -> None:
        """
        Live polling entrypoint: step engine then process emitted events.
        """
        events = await self.engine.step()
        if events:
            await self.process(events)

    # ------------------------------------------------------------------ #
    # Simulation API (Runner-driven)
    # ------------------------------------------------------------------ #

    async def bootstrap(
        self,
        symbol_or_seed: Any,
        seed: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> None:
        """
        One-time initialization from seed candles.

        seed_data shape:
          { "symbol": str, "as_of": str, "candles": { tf: [enriched_candles...] } }

        Supports both signatures:
          - bootstrap(seed_data)
          - bootstrap(symbol, seed)
        """
        if isinstance(symbol_or_seed, dict) and seed is None:
            seed_data = symbol_or_seed
        else:
            symbol = str(symbol_or_seed or "").upper()
            seed_data = {"symbol": symbol, "candles": seed or {}}

        await self._bootstrap_from_seed_data(seed_data)

    async def _bootstrap_from_seed_data(self, seed_data: Dict[str, Any]) -> None:
        self._bootstrap_calls += 1
        symbol = (seed_data.get("symbol") or "").upper()
        candles_by_tf: Dict[str, List[Dict[str, Any]]] = seed_data.get("candles") or {}
        if not symbol or not candles_by_tf:
            raise ValueError("bootstrap requires seed_data with symbol and candles")

        # Initialize internal maps
        self.last_processed_ts.setdefault(symbol, {})
        self.last_snapshots.setdefault(symbol, {})
        self.last_events_state.setdefault(symbol, {})
        self.last_fvgs_cache.setdefault(symbol, {})

        # Compute initial snapshots for all timeframes with enough candles
        for tf in self.timeframes:
            candles = candles_by_tf.get(tf) or []
            if len(candles) < 10:
                continue

            # In simulation, seed candles are assumed closed and ordered
            closed_candles = candles
            self._sim_candles[(symbol, tf)] = list(closed_candles)

            # Session candles (advanced extras)
            last_candle = closed_candles[-1]
            session_date = last_candle.get("date_et")
            if session_date:
                session_candles = [c for c in closed_candles if c.get("date_et") == session_date]
            else:
                session_candles = closed_candles

            snapshot = compute_all_indicators(closed_candles)
            self._calc1_calls_by_tf[tf] = self._calc1_calls_by_tf.get(tf, 0) + 1
            # Diagnostics: bootstrap also calls calc1/calc2 (seed warmup)
            advanced = compute_advanced_extras(
                candles=closed_candles,
                base_snapshot=snapshot,
                htf_snapshot=None,
                session_candles=session_candles,
            )
            self._calc2_calls_by_tf[tf] = self._calc2_calls_by_tf.get(tf, 0) + 1

            # Match simulated row shape used elsewhere.
            snapshot["extras_advanced"] = advanced

            # Multi-TF VP enrichment map will be built after collecting all snapshots
            self.last_snapshots[symbol][tf] = snapshot
            ts_dt = _ensure_dt(last_candle.get("ts"))
            self.last_processed_ts[symbol][tf] = ts_dt

            self.spot_tf_cache[(symbol, tf)] = {
                "symbol": symbol,
                "timeframe": tf,
                "asof": ts_dt.isoformat(),
                "snapshot": snapshot,
                "extras_advanced": advanced,
                "strategies": [],
            }

        # Run VP enrichment once with the multi-TF map
        snap_map: Dict[str, Dict[str, Any]] = dict(self.last_snapshots.get(symbol, {}))
        for tf, snapshot in snap_map.items():
            try:
                _enrich_vp_for_tf(current_tf=tf, current_snapshot=snapshot, snapshots_by_tf=snap_map)
            except Exception as e:
                pass

        # Liquidity pool rebuild once after bootstrap snapshots exist
        self._maybe_rebuild_liquidity_pool(symbol=symbol, trigger_tf="bootstrap")

    async def on_candle(self, symbol: str, timeframe: str, candle: Dict[str, Any]) -> None:
        """
        Push one candle into the bot (simulation mode).
        """
        sym = (symbol or "").upper()
        tf = timeframe
        if not sym or not tf:
            return

        asof = candle.get("ts") or candle.get("asof")
        key = (sym, tf)
        if asof and self._last_asof.get(key) == str(asof):
            return
        if asof:
            self._last_asof[key] = str(asof)

        self._on_candle_total += 1
        self._on_candle_by_tf[tf] = self._on_candle_by_tf.get(tf, 0) + 1

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

        day_et = candle.get("date_et")
        if day_et:
            self._last_day_et_by_symbol[sym] = str(day_et)

        candles = self._sim_candles.setdefault(key, [])
        candles.append(candle)

        # Keep bounded history to avoid unbounded growth.
        if len(candles) > 3000:
            del candles[:-3000]

        if len(candles) < 10:
            self.spot_tf_cache[key] = {
                "symbol": sym,
                "timeframe": tf,
                "asof": str(asof) if asof else None,
                "candle": candle,
            }
            return

        closed_candles = candles
        last_candle = closed_candles[-1]
        session_date = last_candle.get("date_et")
        if session_date:
            session_candles = [c for c in closed_candles if c.get("date_et") == session_date]
        else:
            session_candles = closed_candles

        t_ind0 = time.perf_counter()
        snapshot = compute_all_indicators(closed_candles)
        self._calc1_calls_by_tf[tf] = self._calc1_calls_by_tf.get(tf, 0) + 1
        advanced = compute_advanced_extras(
            candles=closed_candles,
            base_snapshot=snapshot,
            htf_snapshot=None,
            session_candles=session_candles,
        )
        self._calc2_calls_by_tf[tf] = self._calc2_calls_by_tf.get(tf, 0) + 1
        indicators_ms = (time.perf_counter() - t_ind0) * 1000.0
        snapshot["extras_advanced"] = advanced

        self.last_snapshots.setdefault(sym, {})[tf] = snapshot
        ts_dt = _ensure_dt(last_candle.get("ts") or asof)
        self.last_processed_ts.setdefault(sym, {})[tf] = ts_dt

        # Liquidity pool rebuild on each processed candle (like live flow, if enabled)
        self._maybe_rebuild_liquidity_pool(symbol=sym, trigger_tf=tf)

        t_strat0 = time.perf_counter()
        strategies = evaluate_strategies(
            symbol=sym,
            timeframe=tf,
            candles=closed_candles,
            swings=snapshot.get("swings") or {},
            fvgs=snapshot.get("fvgs") or [],
            liquidity=snapshot.get("liquidity") or {},
            trend=snapshot.get("trend") or {},
            cluster=last_candle.get("cluster") or {},
            volume_profile=snapshot.get("volume_profile") or {},
            htf_swings=None,
            structure_states_by_tf={
                k: (v or {}).get("structure_state")
                for k, v in (self.last_snapshots.get(sym, {}) or {}).items()
                if isinstance(v, dict)
            },
        )
        strategies_ms = (time.perf_counter() - t_strat0) * 1000.0

        # ---------------- EVENTS (ensure sim path scans + logs events) ----------------
        self.last_events_state.setdefault(sym, {})
        self.last_fvgs_cache.setdefault(sym, {})
        self.liq_pool_cache.setdefault(sym, None)

        prev_state = self.last_events_state.get(sym, {}).get(tf) or {}
        prev_latest = prev_state.get("events_latest")
        prev_active = prev_state.get("events_active")
        prev_recent = prev_state.get("events_recent")

        fvgs_now = snapshot.get("fvgs") or []
        fvgs_prev = self.last_fvgs_cache.get(sym, {}).get(tf)
        liq_row = self.liq_pool_cache.get(sym)

        # ---------------- LIQUIDITY POOL DEBUG (ONCE) ----------------
        key_lp = (sym, tf)
        if liq_row and not self._logged_liqpool_once.get(key_lp):
            self._logged_liqpool_once[key_lp] = True

        swing_items = (snapshot.get("swings") or {}).get("swings") or []
        swing_highs = [{"ts": s.get("ts"), "price": s.get("price")} for s in swing_items if s.get("type") == "swing_high" and s.get("state") == "active"]
        swing_lows = [{"ts": s.get("ts"), "price": s.get("price")} for s in swing_items if s.get("type") == "swing_low" and s.get("state") == "active"]

        structural_points = (snapshot.get("structural") or {}).get("points") or []
        structural_highs = [{"ts": p.get("ts"), "price": p.get("price")} for p in structural_points if p.get("type") == "swing_high" and p.get("label") in ("HH", "LH")]
        structural_lows = [{"ts": p.get("ts"), "price": p.get("price")} for p in structural_points if p.get("type") == "swing_low" and p.get("label") in ("LL", "HL")]

        events_ms = 0.0
        if ENABLE_SPOT_EVENTS:
            t_ev0 = time.perf_counter()
            ev_ctx = SpotEventContext(
                symbol=sym,
                timeframe=tf,
                last_candle=last_candle,
                liquidity_pool_row=liq_row,
                structural_highs=structural_highs,
                structural_lows=structural_lows,
                swing_highs=swing_highs,
                swing_lows=swing_lows,
                structure_state=(snapshot.get("structure_state") or ""),
                fvgs_now=fvgs_now,
                fvgs_prev=fvgs_prev,
                prev_events_latest=prev_latest,
                prev_events_active=prev_active,
                prev_events_recent=prev_recent,
            )
            ev_out = compute_spot_events(ev_ctx)
            events_ms = (time.perf_counter() - t_ev0) * 1000.0
            self._diag_spot_event_calls_by_tf[str(tf)] += 1
            self._spot_event_calls_by_tf[tf] = self._spot_event_calls_by_tf.get(tf, 0) + 1

            self.last_events_state.setdefault(sym, {})[tf] = {
                "events_latest": ev_out.get("events_latest"),
                "events_active": ev_out.get("events_active"),
                "events_recent": ev_out.get("events_recent"),
            }
        else:
            ev_out = {"events_latest": {}, "events_active": {}, "events_recent": []}
            self.last_events_state.setdefault(sym, {})[tf] = ev_out
        self.last_fvgs_cache.setdefault(sym, {})[tf] = list(fvgs_now)

        row = {
            "symbol": sym,
            "timeframe": tf,
            "asof": ts_dt.isoformat(),
            "snapshot": snapshot,
            "extras_advanced": advanced,
            "strategies": strategies,
            "events": self.last_events_state[sym][tf],
            "timing_ms": {
                "db_calls": 0.0,
                "indicator_calc": indicators_ms,
                "strategy": strategies_ms,
                "events": events_ms,
            },
        }
        self.spot_tf_cache[key] = row
        self._stage_timing_ms[f"{sym}:{tf}"] = row["timing_ms"]

        # ---------------- LOGGING RULES ----------------
        self._log_indicators_once(sym, tf, ts_dt, snapshot, advanced, strategies)
        # Track counts only; do NOT print events live
        self._track_event_counts(sym, tf, ts_dt, ev_out)

        if not self.sim_mode:
            await update_indicators_in_spot_tf(
                symbol=sym,
                timeframe=tf,
                trend=snapshot.get("trend") or {},
                pivots=snapshot.get("pivots") or {},
                swings=snapshot.get("swings") or {},
                structural=snapshot.get("structural") or {},
                fvgs=snapshot.get("fvgs") or [],
                liquidity=snapshot.get("liquidity") or {},
                volume_profile=snapshot.get("volume_profile") or {},
                extras=snapshot.get("extras") or {},
                extras_advanced=advanced,
                structure_state=snapshot.get("structure_state"),
                strategies=strategies,
            )

    # ------------------------------------------------------------------ #
    # Core update logic
    # ------------------------------------------------------------------ #

    async def process(self, events: Dict[str, Any]) -> None:
        """
        Process incremental candle events (Runner-driven).

        events shape:
          {
            "symbol": str,
            "sim_clock": str,
            "new": { tf: [candle...] }
          }

        CandleEngine guarantees that any candles emitted here are closed for their timeframe.
        """
        sym_upper = (events.get("symbol") or "").upper()
        if not sym_upper:
            return

        new_map: Dict[str, List[Dict[str, Any]]] = (events.get("new") or {})
        if not new_map:
            return

        # Determine which TFs changed this step
        changed_tfs = [tf for tf, lst in new_map.items() if lst]
        if not changed_tfs:
            return

        self.last_processed_ts.setdefault(sym_upper, {})
        self.last_snapshots.setdefault(sym_upper, {})
        self.last_events_state.setdefault(sym_upper, {})
        self.last_fvgs_cache.setdefault(sym_upper, {})

        pending: Dict[str, Dict[str, Any]] = {}

        # Build pending packs for changed TFs only
        for tf in changed_tfs:
            if tf not in self.timeframes:
                continue
            t_db0 = time.perf_counter()
            candles = self.engine.get_candles(sym_upper, tf)
            db_calls_ms = (time.perf_counter() - t_db0) * 1000.0
            if not candles or len(candles) < 10:
                continue

            closed_candles = candles  # simulation: engine emits closed tf candles only
            last_candle = closed_candles[-1]
            ts_dt = _ensure_dt(last_candle.get("ts"))

            prev_ts = self.last_processed_ts.get(sym_upper, {}).get(tf)
            if prev_ts is not None and ts_dt <= prev_ts:
                continue

            # Session candles (advanced extras)
            session_date = last_candle.get("date_et")
            if session_date:
                session_candles = [c for c in closed_candles if c.get("date_et") == session_date]
            else:
                session_candles = closed_candles

            t_ind0 = time.perf_counter()
            snapshot = compute_all_indicators(closed_candles)
            advanced = compute_advanced_extras(
                candles=closed_candles,
                base_snapshot=snapshot,
                htf_snapshot=None,
                session_candles=session_candles,
            )
            indicator_ms = (time.perf_counter() - t_ind0) * 1000.0

            pending[tf] = {
                "closed_candles": closed_candles,
                "session_candles": session_candles,
                "snapshot": snapshot,
                "extras_advanced": advanced,
                "last_candle": last_candle,
                "ts_dt": ts_dt,
                "db_calls_ms": db_calls_ms,
                "indicator_ms": indicator_ms,
            }

        if not pending:
            return

        # Multi-TF snapshot map (use cached snapshots for TFs not updated this step)
        snap_map: Dict[str, Dict[str, Any]] = {}
        snap_map.update(self.last_snapshots.get(sym_upper, {}))
        for tf, pack in pending.items():
            snap_map[tf] = pack["snapshot"]

        # Enrich VP for pending TFs with multi-TF context
        for tf, pack in pending.items():
            try:
                _enrich_vp_for_tf(current_tf=tf, current_snapshot=pack["snapshot"], snapshots_by_tf=snap_map)
            except Exception as e:
                pass

        # Update caches
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

            t_strat0 = time.perf_counter()
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
            strategy_ms = (time.perf_counter() - t_strat0) * 1000.0

            # ---------------- EVENTS (cache-only) ----------------
            prev_state = self.last_events_state.get(sym_upper, {}).get(tf) or {}
            prev_latest = prev_state.get("events_latest")
            prev_active = prev_state.get("events_active")
            prev_recent = prev_state.get("events_recent")

            fvgs_prev = self.last_fvgs_cache.get(sym_upper, {}).get(tf)
            liq_row = self.liq_pool_cache.get(sym_upper)

            swing_items = (swings or {}).get("swings") or []
            swing_highs = [
                {"ts": s.get("ts"), "price": s.get("price")}
                for s in swing_items
                if s.get("type") == "swing_high" and s.get("state") == "active"
            ]
            swing_lows = [
                {"ts": s.get("ts"), "price": s.get("price")}
                for s in swing_items
                if s.get("type") == "swing_low" and s.get("state") == "active"
            ]

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

            events_ms = 0.0
            if ENABLE_SPOT_EVENTS:
                t_ev0 = time.perf_counter()
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
                ev_out = compute_spot_events(ev_ctx)
                events_ms = (time.perf_counter() - t_ev0) * 1000.0
                self._diag_spot_event_calls_by_tf[str(tf)] += 1
            else:
                ev_out = {"events_latest": {}, "events_active": {}, "events_recent": []}

            self.last_events_state.setdefault(sym_upper, {})[tf] = {
                "events_latest": ev_out.get("events_latest"),
                "events_active": ev_out.get("events_active"),
                "events_recent": ev_out.get("events_recent"),
            }
            self.last_fvgs_cache.setdefault(sym_upper, {})[tf] = list(fvgs or [])

            self.last_snapshots.setdefault(sym_upper, {})[tf] = snapshot
            self.last_processed_ts.setdefault(sym_upper, {})[tf] = ts_dt

            self.spot_tf_cache[(sym_upper, tf)] = {
                "symbol": sym_upper,
                "timeframe": tf,
                "asof": ts_dt.isoformat(),
                "snapshot": snapshot,
                "extras_advanced": extras_advanced,
                "strategies": strategies,
                "events": self.last_events_state[sym_upper][tf],
                "timing_ms": {
                    "db_calls": float(pack.get("db_calls_ms", 0.0)),
                    "indicator_calc": float(pack.get("indicator_ms", 0.0)),
                    "strategy": strategy_ms,
                    "events": events_ms,
                },
            }
            self._stage_timing_ms[f"{sym_upper}:{tf}"] = self.spot_tf_cache[(sym_upper, tf)]["timing_ms"]
