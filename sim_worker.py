import os
import asyncio
import datetime as dt
import uuid
import logging
import io
from pathlib import Path
from contextlib import contextmanager
from typing import Any, Dict, Optional

import httpx

from candle_engine import CandleEngine
from indicator_bot import IndicatorBot
from liquidity_pool_builder import print_last_liquidity_output
from strategy_bos_fvg import print_bos_fvg_final_summaries as print_bos_fvg_htf_final_summaries
from strategy_bos_fvg_ltf import print_bos_fvg_final_summaries as print_bos_fvg_ltf_final_summaries
import spot_event as spot_event_module


LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [sim_worker] %(message)s",
)
logger = logging.getLogger("sim_worker")
logger.disabled = True  # Logs disabled; keep strategy logs only
# Silence per-request HTTP client logs such as:
# "HTTP Request: POST ... \"HTTP/1.1 200 OK\""
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

DEFAULT_SEED_COUNTS = {
    "1m": 5000,
    "3m": 2500,
    "5m": 1500,
    "15m": 600,
    "1h": 400,
    "1d": 200,
    "1w": 50,
}

SEED_COUNT_ENV_MAP = {
    "1m": "SEED_1M_CANDLES",
    "3m": "SEED_3M_CANDLES",
    "5m": "SEED_5M_CANDLES",
    "15m": "SEED_15M_CANDLES",
    "1h": "SEED_1H_CANDLES",
    "1d": "SEED_1D_CANDLES",
    "1w": "SEED_1W_CANDLES",
}


def _load_seed_counts_from_env() -> Dict[str, int]:
    """Build per-timeframe seed limits from env vars, with safe defaults."""
    out: Dict[str, int] = {}
    for tf, default in DEFAULT_SEED_COUNTS.items():
        env_name = SEED_COUNT_ENV_MAP[tf]
        raw = os.getenv(env_name)
        if raw is None or raw == "":
            out[tf] = int(default)
            continue
        try:
            val = int(raw)
            if val <= 0:
                raise ValueError("must be > 0")
            out[tf] = val
        except Exception:
            logger.warning(
                "Invalid %s=%r; using default %s=%d",
                env_name,
                raw,
                tf,
                default,
            )
            out[tf] = int(default)
    return out


# ----------------------------- Supabase REST -----------------------------

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
    }


async def _sb_select_one(
    client: httpx.AsyncClient,
    base_url: str,
    key: str,
    table: str,
    params: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    endpoint = f"{base_url}/rest/v1/{table}"
    hdrs = _sb_headers(key)
    # Prefer PostgREST "single object" semantics
    hdrs["Accept"] = "application/vnd.pgrst.object+json"
    r = await client.get(endpoint, headers=hdrs, params=params, timeout=30.0)
    if r.status_code == 406:
        # No rows matched (PostgREST returns 406 for object+json when empty)
        return None
    r.raise_for_status()
    return r.json()


async def _sb_patch(
    client: httpx.AsyncClient,
    base_url: str,
    key: str,
    table: str,
    params: Dict[str, str],
    payload: Dict[str, Any],
    *,
    returning: str = "representation",
) -> Any:
    endpoint = f"{base_url}/rest/v1/{table}"
    hdrs = _sb_headers(key)
    hdrs["Prefer"] = f"return={returning}"
    r = await client.patch(endpoint, headers=hdrs, params=params, json=payload, timeout=30.0)
    r.raise_for_status()
    # representation returns json array (or object), minimal returns empty
    return r.json() if r.text else None


async def _sb_insert(
    client: httpx.AsyncClient,
    base_url: str,
    key: str,
    table: str,
    payload: Dict[str, Any],
    *,
    returning: str = "representation",
) -> Any:
    endpoint = f"{base_url}/rest/v1/{table}"
    hdrs = _sb_headers(key)
    hdrs["Prefer"] = f"return={returning}"
    r = await client.post(endpoint, headers=hdrs, json=payload, timeout=30.0)
    r.raise_for_status()
    return r.json() if r.text else None


async def _sb_upload_storage_file(
    client: httpx.AsyncClient,
    base_url: str,
    key: str,
    bucket: str,
    object_path: str,
    local_file_path: Path,
) -> None:
    endpoint = f"{base_url}/storage/v1/object/{bucket}/{object_path}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "x-upsert": "true",
        "Content-Type": "text/plain; charset=utf-8",
    }
    with local_file_path.open("rb") as fh:
        r = await client.post(endpoint, headers=headers, content=fh.read(), timeout=60.0)
    r.raise_for_status()


def _make_log_run_id() -> str:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:6]
    return f"{ts}_{suffix}"


class _TeeStream(io.TextIOBase):
    def __init__(self, primary: io.TextIOBase, mirror: io.TextIOBase) -> None:
        self._primary = primary
        self._mirror = mirror

    def write(self, s: str) -> int:
        written = self._primary.write(s)
        self._mirror.write(s)
        self._primary.flush()
        self._mirror.flush()
        return written

    def flush(self) -> None:
        self._primary.flush()
        self._mirror.flush()


@contextmanager
def _capture_stdout_to_file(log_file_path: Path):
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    with log_file_path.open("a", encoding="utf-8", buffering=1) as fh:
        original_stdout = os.sys.stdout
        tee = _TeeStream(original_stdout, fh)
        os.sys.stdout = tee
        try:
            yield
        finally:
            tee.flush()
            os.sys.stdout = original_stdout


def _to_iso_utc(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if isinstance(value, dt.datetime):
            out = value
        else:
            out = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if out.tzinfo is None:
            out = out.replace(tzinfo=dt.timezone.utc)
        return out.astimezone(dt.timezone.utc).isoformat()
    except Exception:
        return None


def _normalize_trade(trade: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "entry_ts": _to_iso_utc(trade.get("entry_fill_timestamp") or trade.get("entry_signal_timestamp")),
        "exit_ts": _to_iso_utc(trade.get("final_exit_timestamp") or trade.get("exit_timestamp")),
        "side": trade.get("side"),
        "entry_price": trade.get("entry_price"),
        "exit_price": trade.get("final_exit_price") or trade.get("exit_price"),
        "quantity": trade.get("position_size"),
        "pnl": trade.get("net_pnl"),
        "reason_exit": trade.get("exit_reason"),
    }


def _build_event_payloads() -> tuple[Dict[str, int], Dict[str, list[str]]]:
    trig_counts = getattr(spot_event_module, "_SPOT_EVENT_TRIGGER_COUNTS", {}) or {}
    trig_ts = getattr(spot_event_module, "_SPOT_EVENT_TRIGGER_TS", {}) or {}

    event_counters: Dict[str, int] = {
        "bos_up": int(trig_counts.get("structure_break_up", 0) or 0),
        "bos_down": int(trig_counts.get("structure_break_down", 0) or 0),
        "choch_up": int(trig_counts.get("choch_up", 0) or 0),
        "choch_down": int(trig_counts.get("choch_down", 0) or 0),
        "displacement_up": int(trig_counts.get("displacement_up", 0) or 0),
        "displacement_down": int(trig_counts.get("displacement_down", 0) or 0),
        "liquidity_sweep": int(trig_counts.get("liquidity_sweep_high", 0) or 0)
        + int(trig_counts.get("liquidity_sweep_low", 0) or 0),
    }

    def _event_ts_list(key: str) -> list[str]:
        out: list[str] = []
        for row in (trig_ts.get(key) or []):
            if not isinstance(row, dict):
                continue
            ts_iso = _to_iso_utc(row.get("candle_ts"))
            if ts_iso:
                out.append(ts_iso)
        return sorted(list(set(out)))

    event_candles: Dict[str, list[str]] = {
        "bos_up": _event_ts_list("structure_break_up"),
        "bos_down": _event_ts_list("structure_break_down"),
        "choch_up": _event_ts_list("choch_up"),
        "choch_down": _event_ts_list("choch_down"),
        "displacement_up": _event_ts_list("displacement_up"),
        "displacement_down": _event_ts_list("displacement_down"),
        "liquidity_sweep": sorted(
            list(set(_event_ts_list("liquidity_sweep_high") + _event_ts_list("liquidity_sweep_low")))
        ),
    }
    return event_counters, event_candles


def _build_trades_payload(bot: IndicatorBot, symbol: str) -> tuple[Dict[str, Any], list[Dict[str, Any]]]:
    sym = (symbol or "").upper()
    tf_map = bot._sim_strategy_results.get(sym, {}) if hasattr(bot, "_sim_strategy_results") else {}

    picked: Optional[Dict[str, Any]] = None
    for tf in ("1m", "5m", "3m", "15m", "1h", "1d", "1w"):
        result = tf_map.get(tf)
        if isinstance(result, dict):
            picked = result
            break
    if picked is None:
        for result in tf_map.values():
            if isinstance(result, dict):
                picked = result
                break

    perf = (picked or {}).get("performance") if isinstance(picked, dict) else {}
    perf = perf if isinstance(perf, dict) else {}
    trade_log = (picked or {}).get("trade_log") if isinstance(picked, dict) else []
    trade_log = trade_log if isinstance(trade_log, list) else []

    trades_summary: Dict[str, Any] = {
        "total_trades": int(perf.get("total_trades", len(trade_log)) or 0),
        "wins": int(perf.get("winning_trades", 0) or 0),
        "losses": int(perf.get("losing_trades", 0) or 0),
        "win_rate": round(float(perf.get("win_rate", 0.0) or 0.0) * 100.0, 2),
        "gross_profit": float(perf.get("gross_profit", 0.0) or 0.0),
        "gross_loss": -abs(float(perf.get("gross_loss", 0.0) or 0.0)),
        "net_pnl": float(perf.get("net_profit", 0.0) or 0.0),
        "max_drawdown": -abs(float(perf.get("max_drawdown", 0.0) or 0.0)),
    }
    trades = [_normalize_trade(t) for t in trade_log if isinstance(t, dict)]
    return trades_summary, trades


async def _create_simulation_run(
    client: httpx.AsyncClient,
    run_id: str,
    symbol: str,
    seed_date: str,
    sim_period: int,
) -> None:
    base_url, key = _sb_env()
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    full_payload: Dict[str, Any] = {
        "id": run_id,
        "status": "running",
        "start_time": now,
        "symbol": symbol,
        "timeframe": "multi",
        "strategy_name": "SPY_VWAP_Pullback_Scalp_Sim",
        "strategy_version": "v1.0",
        "event_counters": {
            "bos_up": 0,
            "bos_down": 0,
            "choch_up": 0,
            "choch_down": 0,
            "displacement_up": 0,
            "displacement_down": 0,
            "liquidity_sweep": 0,
        },
        "event_candles": {
            "bos_up": [],
            "bos_down": [],
            "choch_up": [],
            "choch_down": [],
            "displacement_up": [],
            "displacement_down": [],
            "liquidity_sweep": [],
        },
        "trades_summary": {},
        "trades": [],
        "config": {
            "symbol": symbol,
            "seed_date": seed_date,
            "sim_period": sim_period,
        },
        "error_message": None,
        "updated_at": now,
    }

    fallback_payload: Dict[str, Any] = {
        "id": run_id,
        "status": "running",
        "start_time": now,
        "symbol": symbol,
        "timeframe": "multi",
        "strategy_name": "SPY_VWAP_Pullback_Scalp_Sim",
        "strategy_version": "v1.0",
        "error_message": None,
        "updated_at": now,
    }

    try:
        await _sb_insert(
            client,
            base_url,
            key,
            "simulation_runs",
            payload=full_payload,
            returning="minimal",
        )
    except httpx.HTTPStatusError as e:
        # Some deployments have a reduced schema and reject one or more JSON columns.
        # Retry with a strict minimal payload so run tracking is still created.
        body = e.response.text[:500] if e.response is not None else str(e)
        logger.warning("full simulation_runs insert failed (retrying minimal payload): %s", body)
        await _sb_insert(
            client,
            base_url,
            key,
            "simulation_runs",
            payload=fallback_payload,
            returning="minimal",
        )


async def _update_simulation_run(
    client: httpx.AsyncClient,
    run_id: str,
    payload: Dict[str, Any],
) -> None:
    base_url, key = _sb_env()
    body = dict(payload)
    body["updated_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    await _sb_patch(
        client,
        base_url,
        key,
        "simulation_runs",
        params={"id": f"eq.{run_id}"},
        payload=body,
        returning="minimal",
    )


async def _claim_one_job(client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    """
    Claim exactly one sim_ticker row where start_sim='y'.

    We do this in 2 steps (read -> patch) because we’re on REST.
    It’s not perfectly atomic like SQL, but it’s good enough for a single-run worker.
    """
    base_url, key = _sb_env()

    # 1) Find one candidate job
    job = await _sb_select_one(
        client,
        base_url,
        key,
        "sim_ticker",
        params={
            "select": "*",
            "start_sim": "eq.y",
            "limit": "1",
        },
    )
    if not job:
        return None

    symbol_db = str(job.get("symbol") or "")
    symbol = symbol_db.upper()
    run_id = str(uuid.uuid4())
    now = dt.datetime.now(dt.timezone.utc).isoformat()

    # 2) Patch it to running + flip start_sim to 'n'
    updated = await _sb_patch(
        client,
        base_url,
        key,
        "sim_ticker",
        params={"symbol": f"eq.{symbol_db}"},
        payload={
            "start_sim": "n",
            "status": "running",
            "run_id": run_id,
            "started_at": now,
            "finished_at": None,
            "error_message": None,
        },
        returning="representation",
    )

    # PostgREST returns a list for PATCH with representation
    if isinstance(updated, list):
        if not updated:
            logger.warning("claim patch matched zero sim_ticker rows for symbol=%s", symbol_db)
            return None
        out = dict(updated[0])
    elif isinstance(updated, dict):
        out = dict(updated)
    else:
        logger.warning("claim patch returned unexpected payload type=%s for symbol=%s", type(updated).__name__, symbol_db)
        return None

    out["_symbol_db"] = symbol_db
    out["run_id"] = str(out.get("run_id") or run_id)
    return out


async def _mark_done(client: httpx.AsyncClient, symbol: str) -> None:
    base_url, key = _sb_env()
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    await _sb_patch(
        client,
        base_url,
        key,
        "sim_ticker",
        params={"symbol": f"eq.{symbol}"},
        payload={"status": "done", "finished_at": now},
        returning="minimal",
    )


async def _mark_error(client: httpx.AsyncClient, symbol: str, msg: str) -> None:
    base_url, key = _sb_env()
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    await _sb_patch(
        client,
        base_url,
        key,
        "sim_ticker",
        params={"symbol": f"eq.{symbol}"},
        payload={"status": "error", "error_message": msg[:2000], "finished_at": now},
        returning="minimal",
    )


# ----------------------------- Worker Main -----------------------------

async def main() -> int:
    logger.info("sim worker booting")
    # quick env validation early
    try:
        _sb_env()
    except Exception as e:
        logger.exception("supabase env validation failed: %s", e)
        return 1

    async with httpx.AsyncClient() as client:
        job = await _claim_one_job(client)
        if not job:
            logger.info("no sim_ticker rows with start_sim='y'; worker exiting")
            return 0

        symbol_db = str(job.get("_symbol_db") or job.get("symbol") or "")
        symbol = symbol_db.upper()
        seed_date = job.get("seed_date")  # expected 'YYYY-MM-DD' in ET
        sim_period = int(job.get("sim_period") or 0)  # days
        run_id = str(job.get("run_id") or "")
        log_date = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
        log_run_id = _make_log_run_id()
        log_local_path = Path("simulation_logs") / symbol / log_date / f"run_{log_run_id}.txt"
        log_storage_path = f"{symbol}/{log_date}/run_{log_run_id}.txt"

        if not symbol or not seed_date or sim_period <= 0:
            msg = f"Invalid job fields: symbol={symbol!r} seed_date={seed_date!r} sim_period={sim_period!r}"
            logger.error(msg)
            await _mark_error(client, symbol_db or "UNKNOWN", msg)
            return 1

        logger.info(
            "claimed job symbol=%s seed_date=%s sim_period=%s run_id=%s",
            symbol,
            seed_date,
            sim_period,
            run_id,
        )

        try:
            with _capture_stdout_to_file(log_local_path):
                print(f"[SIM_LOG] local log capture path={log_local_path.as_posix()}")
                print(f"[SIM_LOG] storage target path=Simulation_runs/{log_storage_path}")
                # Candle engine reads candles from DB via Supabase REST (we’ll patch candle_engine next)
                engine = CandleEngine(symbols=[symbol])

                # Indicator bot in simulation mode (no DB writes)
                bot = IndicatorBot(engine=engine, sim_mode=True)

                # Seed counts, configurable via env vars (SEED_*_CANDLES)
                seed_counts = _load_seed_counts_from_env()

                seed = await engine.load_seed_from_db(symbol=symbol, seed_date_et=seed_date, counts=seed_counts)
                await bot.bootstrap(symbol, seed)
                logger.info("seeded simulation data for symbol=%s", symbol)

            # ---------------- SEED LOGS ----------------
                def _ts_str(x: Any) -> str:
                    try:
                        if isinstance(x, dt.datetime):
                            t = x
                        else:
                            t = dt.datetime.fromisoformat(str(x))
                        if t.tzinfo is None:
                            t = t.replace(tzinfo=dt.timezone.utc)
                        return t.astimezone(dt.timezone.utc).isoformat()
                    except Exception:
                        return str(x)

            # print("[SIM][SEED] Seed candle stats (UTC):")
                for tf in sorted(seed_counts.keys(), key=lambda s: (len(s), s)):
                    arr = (seed or {}).get(tf) or []
                    n = len(arr)
                    if n == 0:
                        # print(f"[SIM][SEED] {symbol} {tf}: n=0")
                        continue
                    first_ts = _ts_str(arr[0].get("ts"))
                    last_ts = _ts_str(arr[-1].get("ts"))
                    # print(f"[SIM][SEED] {symbol} {tf}: n={n} first_ts={first_ts} last_ts={last_ts}")

            # Run sim day-by-day starting next trading day 09:30 ET
                sim_days = await engine.get_sim_days(symbol=symbol, start_after_seed_date_et=seed_date, num_days=sim_period)
            # print(f"[SIM_WORKER] Sim days: {sim_days[:3]}{'...' if len(sim_days) > 3 else ''}")

            # ---------------- LIVE SIM LOGS ----------------
                first_live_ts: Dict[str, str] = {}
                last_live_ts: Dict[str, str] = {}

                first_live_to_bot: Optional[Dict[str, Any]] = None
                last_live_to_bot: Optional[Dict[str, Any]] = None
                emitted_events = 0

                for d in sim_days:
                    # IMPORTANT: stream_day() now behaves like live:
                    # - reads ONLY 1m from DB
                    # - enriches 1m via CandleEngine._enrich_candle()
                    # - aggregates 3m/5m/15m/1h from 1m via _aggregate_from_1m()
                    # - emits closed candles for those HTFs
                    async for event in engine.stream_day(symbol=symbol, date_et=d):
                        # event = {"tf": "1m"/"3m"/..., "candle": enriched {...}}
                        tf = event["tf"]
                        candle = event["candle"]
                        try:
                            tf_s = str(tf or "")
                            ts = _ts_str(candle.get("ts"))
                            if tf_s and tf_s not in first_live_ts:
                                first_live_ts[tf_s] = ts
                            if tf_s:
                                last_live_ts[tf_s] = ts
                        except Exception:
                            pass

                        if first_live_to_bot is None:
                            first_live_to_bot = {
                                "tf": tf,
                                "ts": candle.get("ts"),
                                "open": candle.get("open"),
                                "high": candle.get("high"),
                                "low": candle.get("low"),
                                "close": candle.get("close"),
                                "volume": candle.get("volume"),
                            }

                        last_live_to_bot = {
                            "tf": tf,
                            "ts": candle.get("ts"),
                            "open": candle.get("open"),
                            "high": candle.get("high"),
                            "low": candle.get("low"),
                            "close": candle.get("close"),
                            "volume": candle.get("volume"),
                        }

                        await bot.on_candle(symbol=symbol, timeframe=tf, candle=candle)
                        emitted_events += 1

            # print("[SIM][LIVE] Live sim candle range (UTC):")
                for tf in sorted(last_live_ts.keys(), key=lambda s: (len(s), s)):
                    pass
                # print(f"[SIM][LIVE] {symbol} {tf}: first_live_ts={first_live_ts.get(tf)} last_live_ts={last_live_ts.get(tf)}")

            # print(f"[SIM][LIVE] First live candle sent to bot: {first_live_to_bot}")
            # print(f"[SIM][LIVE] Last live candle sent to bot:  {last_live_to_bot}")
            # Compare with CandleEngine emitted stats
                try:
                    emit_counts = engine.get_live_emit_counts(symbol)
                    first_last = engine.get_live_first_last(symbol)
                    logger.info("engine emitted counts by timeframe: %s", emit_counts)
                    logger.info("engine first emitted candle: %s", first_last.get("first"))
                    logger.info("engine last emitted candle: %s", first_last.get("last"))
                except Exception as e:
                    logger.warning("engine diagnostics read failed: %s", e)

            # Print final event summary (totals + per timeframe + per day)
                try:
                    bot.print_event_summary()
                except Exception as e:
                    logger.warning("failed to print event summary: %s", e)

            # Print only end-of-run diagnostics counts
                try:
                    bot.dump_diag_counts(symbol)
                except Exception as e:
                    logger.warning("failed to print diag counts: %s", e)

            # Print ONLY the last liquidity pool output (once per sim run)
                try:
                    print_last_liquidity_output()
                except Exception as e:
                    logger.warning("failed to print final liquidity output: %s", e)

            # Print BOS trades once at the end of the simulation.
                try:
                    print_bos_fvg_htf_final_summaries()
                    print_bos_fvg_ltf_final_summaries()
                except Exception as e:
                    logger.warning("failed to print BOS_FVG final summaries: %s", e)

                print(f"[SIM_LOG] local log file complete path={log_local_path.as_posix()}")

            await _mark_done(client, symbol_db)
            try:
                base_url, key = _sb_env()
                await _sb_upload_storage_file(
                    client=client,
                    base_url=base_url,
                    key=key,
                    bucket="Simulation_runs",
                    object_path=log_storage_path,
                    local_file_path=log_local_path,
                )
                print(f"[SIM_LOG] uploaded log file to Simulation_runs/{log_storage_path}")
            except Exception as upload_err:
                print(
                    f"[SIM_LOG] upload failed local_path={log_local_path.as_posix()} "
                    f"storage_path=Simulation_runs/{log_storage_path} error={upload_err}"
                )
            logger.info("simulation completed successfully for symbol=%s", symbol)
            return 0

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            logger.exception("simulation failed for symbol=%s: %s", symbol, msg)
            try:
                await _mark_error(client, symbol_db or symbol, msg)
            except Exception as e2:
                logger.exception("failed to mark DB error for symbol=%s: %s", symbol, e2)
            return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
