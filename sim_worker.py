import os
import asyncio
import datetime as dt
import uuid
import logging
import io
import sys
import json
from copy import deepcopy
from pathlib import Path
from contextlib import contextmanager
from typing import Any, Dict, Optional
import re

import httpx
from zoneinfo import ZoneInfo
from strategies import get_available_strategy_ids

from candle_engine import CandleEngine, is_rth_now
from indicator_bot import IndicatorBot
from liquidity_pool_builder import print_last_liquidity_output
from strategy_bos_fvg_ltf_sim import (
    print_bos_fvg_final_summaries as print_bos_fvg_ltf_sim_final_summaries,
)
from strategy_bos_fvg_ltf import (
    get_live_bridge_rows as get_bos_fvg_ltf_live_bridge_rows,
    apply_live_bridge_db_state as apply_bos_fvg_ltf_live_bridge_db_state,
    set_bridge_runtime_mode as set_bos_fvg_ltf_runtime_mode,
    get_bridge_runtime_mode as get_bos_fvg_ltf_runtime_mode,
    export_bridge_snapshot_for_symbol as export_bos_fvg_ltf_snapshot_for_symbol,
    restore_bridge_snapshot_for_symbol as restore_bos_fvg_ltf_snapshot_for_symbol,
)
import spot_event as spot_event_module


LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [sim_worker] %(message)s",
)
logger = logging.getLogger("sim_worker")
logger.disabled = True  # Logs disabled; keep strategy logs only
_BRIDGE_NEW_INSERTED_KEYS: set[str] = set()
_BRIDGE_ACTIVE_PATCH_LAST: Dict[str, Dict[str, Any]] = {}
# Silence per-request HTTP client logs such as:
# "HTTP Request: POST ... \"HTTP/1.1 200 OK\""
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
EASTERN = ZoneInfo("America/New_York")

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

SIMULATION_RUNS_ALLOWED_COLUMNS = {
    "id",
    "start_time",
    "end_time",
    "simulation_start_time",
    "simulation_end_time",
    "symbol",
    "strategy_name",
    "strategy_version",
    "status",
    "event_counters",
    "trades_summary",
    "config",
    "error_message",
    "created_at",
    "updated_at",
}


def _sanitize_live_runs_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only columns that exist in live_runs to avoid PostgREST 400 errors."""
    return {k: v for k, v in payload.items() if k in SIMULATION_RUNS_ALLOWED_COLUMNS}


def _parse_iso_dt(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    try:
        if isinstance(value, dt.datetime):
            out = value
        else:
            out = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if out.tzinfo is None:
            out = out.replace(tzinfo=dt.timezone.utc)
        return out.astimezone(dt.timezone.utc)
    except Exception:
        return None


def _iso_utc(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).isoformat()


def _poll_sleep_seconds() -> float:
    raw = (os.getenv("SIM_LIVE_POLL_SECONDS") or "").strip()
    if not raw:
        return 1.0
    try:
        out = float(raw)
        return 0.2 if out <= 0 else out
    except Exception:
        return 1.0


def _supervisor_poll_seconds() -> float:
    raw = (os.getenv("SIM_SUPERVISOR_POLL_SECONDS") or "").strip()
    if not raw:
        return 2.0
    try:
        out = float(raw)
        return 0.5 if out <= 0 else out
    except Exception:
        return 2.0


async def _fetch_new_rth_1m_rows(
    engine: CandleEngine,
    symbol: str,
    after_ts: Optional[dt.datetime],
    upto_ts: Optional[dt.datetime] = None,
    limit: int = 5000,
) -> list[Dict[str, Any]]:
    ts_gte = None
    if after_ts is not None:
        ts_gte = _iso_utc(after_ts + dt.timedelta(microseconds=1))
    ts_lte = _iso_utc(upto_ts) if upto_ts is not None else None
    return await engine._sb_fetch_candles(
        table="candle_history_1m",
        symbol=symbol,
        ts_gte=ts_gte,
        ts_lte=ts_lte,
        session_eq="rth",
        limit=limit,
        order_asc=True,
    )


async def _process_rth_1m_rows(
    *,
    engine: CandleEngine,
    bot: IndicatorBot,
    client: httpx.AsyncClient,
    symbol: str,
    rows_1m: list[Dict[str, Any]],
    bridge_sync_enabled: bool,
    first_live_ts: Dict[str, str],
    last_live_ts: Dict[str, str],
    first_live_to_bot: Optional[Dict[str, Any]],
    last_live_to_bot: Optional[Dict[str, Any]],
) -> tuple[Optional[dt.datetime], Optional[Dict[str, Any]], Optional[Dict[str, Any]], int]:
    sym = symbol.upper()
    emitted_events = 0
    last_processed_ts: Optional[dt.datetime] = None

    engine.candles.setdefault(sym, {})
    engine.latest_ts.setdefault(sym, {})
    engine.candles[sym].setdefault("1m", [])
    engine.latest_ts[sym].setdefault("1m", None)
    for tf in ("3m", "5m", "15m", "1h"):
        engine.candles[sym].setdefault(tf, [])
        engine.latest_ts[sym].setdefault(tf, None)

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

    def _record_event(tf: str, candle: Dict[str, Any]) -> None:
        nonlocal emitted_events, first_live_to_bot, last_live_to_bot
        emitted_events += 1
        ts_str = str(candle.get("ts"))
        if tf not in first_live_ts:
            first_live_ts[tf] = ts_str
        last_live_ts[tf] = ts_str

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

    for r in rows_1m:
        raw_1m = _norm_db_row(r)
        e_1m = engine._enrich_candle(sym, "1m", raw_1m)
        if e_1m.get("session") != "rth":
            continue
        engine.candles[sym]["1m"].append(e_1m)
        ts_dt = _parse_iso_dt(e_1m.get("ts"))
        engine.latest_ts[sym]["1m"] = ts_dt
        engine.sim_clock_ts = ts_dt
        last_processed_ts = ts_dt

        _record_event("1m", e_1m)
        await bot.on_candle(symbol=symbol, timeframe="1m", candle=e_1m)

        for tf in ("3m", "5m", "15m", "1h"):
            new_htf = engine._aggregate_from_1m(sym, tf, [e_1m])
            for c in new_htf:
                _record_event(tf, c)
                await bot.on_candle(symbol=symbol, timeframe=tf, candle=c)

        if bridge_sync_enabled:
            try:
                await _sync_bos_fvg_bridge_rows_to_supabase(client)
            except Exception as bridge_err:
                logger.warning("bridge sync failed during live loop: %s", bridge_err)

    return last_processed_ts, first_live_to_bot, last_live_to_bot, emitted_events



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


async def _sb_select(
    client: httpx.AsyncClient,
    base_url: str,
    key: str,
    table: str,
    params: Dict[str, str],
) -> list[Dict[str, Any]]:
    endpoint = f"{base_url}/rest/v1/{table}"
    hdrs = _sb_headers(key)
    r = await client.get(endpoint, headers=hdrs, params=params, timeout=30.0)
    r.raise_for_status()
    payload = r.json()
    return payload if isinstance(payload, list) else []


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

async def _sb_delete(
    client: httpx.AsyncClient,
    base_url: str,
    key: str,
    table: str,
    params: Dict[str, str],
    *,
    returning: str = "minimal",
) -> Any:
    endpoint = f"{base_url}/rest/v1/{table}"
    hdrs = _sb_headers(key)
    hdrs["Prefer"] = f"return={returning}"
    r = await client.delete(endpoint, headers=hdrs, params=params, timeout=30.0)
    r.raise_for_status()
    return r.json() if r.text else None


def _tag_value(tags: Any, prefix: str) -> Optional[str]:
    if not isinstance(tags, list):
        return None
    want = f"{prefix}:"
    for tag in tags:
        if isinstance(tag, str) and tag.startswith(want):
            return tag.split(":", 1)[1].strip() or None
    return None


def _bridge_match_params_active_trades(row: Dict[str, Any]) -> Dict[str, str]:
    tags = row.get("tags") if isinstance(row.get("tags"), list) else []
    setup_id = _tag_value(tags, "id") or str(row.get("setup_id") or "").strip()
    leg = _tag_value(tags, "leg") or str(row.get("leg") or "").strip()
    trade = _tag_value(tags, "trade") or str(row.get("trade") or "").strip()
    return {
        "select": "id,tags,status",
        "tags": f"cs.{{\"id:{setup_id}\",\"leg:{leg}\",\"trade:{trade}\"}}",
    }


def _et_naive_to_utc_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        parsed = dt.datetime.fromisoformat(s)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=EASTERN)
        return parsed.astimezone(dt.timezone.utc).isoformat()
    except Exception:
        return None


def _sanitize_bridge_new_trades_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {
        "symbol",
        "asset_type",
        "cp",
        "qty",
        "entry_type",
        "entry_cond",
        "entry_level",
        "entry_tf",
        "sl_type",
        "sl_cond",
        "sl_level",
        "sl_tf",
        "tp_type",
        "tp_level",
        "note",
        "tags",
        "trade_type",
        "end_time",
        "entry_time",
        "id",
    }
    setup_id = str(payload.get("setup_id") or "").strip()
    out: Dict[str, Any] = {}
    out["note"] = f"bridge:{setup_id}" if setup_id else "bridge:"
    out["trade_type"] = "swing"
    for k in allowed:
        if k in out:
            continue
        if payload.get(k) is not None:
            out[k] = payload.get(k)
    return out


def _sanitize_bridge_active_trades_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {
        "sl_level",
        "manage",
        "note",
    }
    setup_id = str(payload.get("setup_id") or "").strip()
    out: Dict[str, Any] = {
        "note": f"bridge:{setup_id}" if setup_id else "bridge:",
    }
    for k in allowed:
        if k in out:
            continue
        if payload.get(k) is not None:
            out[k] = payload.get(k)
    return out


async def _sync_bos_fvg_bridge_rows_to_supabase(client: httpx.AsyncClient) -> None:
    rows = get_bos_fvg_ltf_live_bridge_rows()
    if not rows:
        return
    base_url, key = _sb_env()
    db_state_updates: list[Dict[str, Any]] = []
    for row in rows:
        tags = row.get("tags") if isinstance(row.get("tags"), list) else []
        setup_id = _tag_value(tags, "id") or str(row.get("setup_id") or "").strip()
        leg = _tag_value(tags, "leg") or str(row.get("leg") or "").strip()
        trade = _tag_value(tags, "trade") or str(row.get("trade") or "").strip()
        if not setup_id or leg is None or trade is None:
            continue
        row_id = f"{setup_id}:{leg}:{trade}"
        active_params = {
            "select": "id,tags,status,manage",
            "tags": f"cs.{{\"id:{setup_id}\",\"leg:{leg}\",\"trade:{trade}\"}}",
        }
        new_params = {
            "select": "id,tags",
            "tags": f"cs.{{\"id:{setup_id}\",\"leg:{leg}\",\"trade:{trade}\"}}",
        }
        new_payload = _sanitize_bridge_new_trades_payload(row)
        db_new_insert_confirmed = False
        db_active_seen = False
        db_active_status = None
        db_active_manage = None

        try:
            active_rows = await _sb_select(client, base_url, key, "active_trades", params=active_params)
        except httpx.HTTPStatusError as e:
            active_rows = []
            body = (e.response.text if e.response is not None else str(e))[:240]
            print(f"[BOS_FVG_BRIDGE][WARN] table=active_trades action=select id={setup_id} leg={leg} trade={trade} err={body}")
        if active_rows:
            db_active_seen = True
            db_active_status = "nt-managing" if any(str(r.get("status") or "") == "nt-managing" for r in active_rows) else "nt-waiting"
            for active_row in active_rows:
                manage_val = active_row.get("manage")
                if manage_val is not None:
                    db_active_manage = manage_val
                    break

        if row_id in _BRIDGE_NEW_INSERTED_KEYS:
            db_new_insert_confirmed = True
        elif db_active_seen:
            db_new_insert_confirmed = True
            _BRIDGE_NEW_INSERTED_KEYS.add(row_id)
        else:
            try:
                existing_new_rows = await _sb_select(client, base_url, key, "new_trades", params=new_params)
            except httpx.HTTPStatusError as e:
                existing_new_rows = []
                body = (e.response.text if e.response is not None else str(e))[:240]
                print(f"[BOS_FVG_BRIDGE][WARN] table=new_trades action=select id={setup_id} leg={leg} trade={trade} err={body}")
            if existing_new_rows:
                db_new_insert_confirmed = True
                _BRIDGE_NEW_INSERTED_KEYS.add(row_id)
            else:
                try:
                    print(
                        f"[BOS_FVG_BRIDGE][DB_WRITE] action=insert table=new_trades "
                        f"id={setup_id} leg={leg} trade={trade} payload={json.dumps(new_payload, default=str, sort_keys=True)}"
                    )
                    await _sb_insert(client, base_url, key, "new_trades", payload=new_payload, returning="minimal")
                    _BRIDGE_NEW_INSERTED_KEYS.add(row_id)
                    db_new_insert_confirmed = True
                    print(f"[BOS_FVG_BRIDGE][DB_APPLIED] action=insert table=new_trades id={setup_id} leg={leg} trade={trade}")
                except httpx.HTTPStatusError as e:
                    body = (e.response.text if e.response is not None else str(e))[:240]
                    print(f"[BOS_FVG_BRIDGE][WARN] table=new_trades id={setup_id} leg={leg} trade={trade} err={body}")

        should_patch_active = False
        active_patch_payload: Dict[str, Any] = {}

        # Phase 2 (armed): only allow explicit cancel via manage="C"
        if db_active_seen and str(row.get("manage") or "") == "C":
            active_patch_payload = _sanitize_bridge_active_trades_payload(
                {
                    "manage": "C",
                    "note": f"bridge:{setup_id}",
                }
            )
            should_patch_active = bool(active_patch_payload)

        # Phase 3 (live): allow live-management fields only when DB says row is live
        elif db_active_seen and str(db_active_status or "") == "nt-managing":
            # Build payload without blindly sending sl_level
            active_patch_payload = {
                "manage": row.get("manage"),
                "note": f"bridge:{setup_id}",
            }

            # Only send sl_level if it actually changed
            current_sl = row.get("sl_level")

            patch_key = f"{setup_id}:{leg}:{trade}"
            last_payload = _BRIDGE_ACTIVE_PATCH_LAST.get(patch_key, {})
            last_sl = last_payload.get("sl_level")

            if current_sl is not None and current_sl != last_sl:
                active_patch_payload["sl_level"] = current_sl

            active_patch_payload = _sanitize_bridge_active_trades_payload(active_patch_payload)
            should_patch_active = bool(active_patch_payload)

        patch_key = f"{setup_id}:{leg}:{trade}"
        if should_patch_active:
            last_payload = _BRIDGE_ACTIVE_PATCH_LAST.get(patch_key)
            if last_payload == active_patch_payload:
                should_patch_active = False

        if should_patch_active:
            try:
                print(
                    f"[BOS_FVG_BRIDGE][DB_WRITE] action=patch table=active_trades "
                    f"id={setup_id} leg={leg} trade={trade} payload={json.dumps(active_patch_payload, default=str, sort_keys=True)}"
                )
                await _sb_patch(
                    client,
                    base_url,
                    key,
                    "active_trades",
                    params=active_params,
                    payload=active_patch_payload,
                    returning="minimal",
                )
                _BRIDGE_ACTIVE_PATCH_LAST[patch_key] = dict(active_patch_payload)
                print(f"[BOS_FVG_BRIDGE][DB_APPLIED] action=patch table=active_trades id={setup_id} leg={leg} trade={trade}")
            except httpx.HTTPStatusError as e:
                body = (e.response.text if e.response is not None else str(e))[:240]
                print(f"[BOS_FVG_BRIDGE][WARN] table=active_trades id={setup_id} leg={leg} trade={trade} err={body}")
        db_state_updates.append(
            {
                "setup_id": setup_id,
                "leg": leg,
                "trade": trade,
                "db_new_insert_confirmed": db_new_insert_confirmed,
                "db_active_seen": db_active_seen,
                "db_active_status": db_active_status,
                "db_active_manage": db_active_manage,
            }
        )
    apply_bos_fvg_ltf_live_bridge_db_state(db_state_updates)


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


def _snapshot_dir() -> Path:
    raw = (os.getenv("SIM_WORKER_SNAPSHOT_DIR") or "sim_worker_snapshots").strip()
    return Path(raw)


def _snapshot_path(symbol: str) -> Path:
    return _snapshot_dir() / f"{str(symbol or '').upper()}.json"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        return {"__type__": "datetime", "value": _iso_utc(value)}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    return value


def _json_restore(value: Any) -> Any:
    if isinstance(value, list):
        return [_json_restore(v) for v in value]
    if isinstance(value, dict):
        marker = value.get("__type__")
        if marker == "datetime":
            return _parse_iso_dt(value.get("value"))
        return {k: _json_restore(v) for k, v in value.items()}
    return value


def _write_snapshot_file(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, default=str, sort_keys=True)
    tmp_path.replace(path)


def _load_local_snapshot(symbol: str) -> Optional[Dict[str, Any]]:
    path = _snapshot_path(symbol)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _delete_local_snapshot(symbol: str) -> None:
    path = _snapshot_path(symbol)
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


def _build_worker_snapshot(
    *,
    symbol: str,
    run_id: str,
    seed_date: str,
    sim_period: int,
    engine: CandleEngine,
    first_live_ts: Dict[str, str],
    last_live_ts: Dict[str, str],
    first_live_to_bot: Optional[Dict[str, Any]],
    last_live_to_bot: Optional[Dict[str, Any]],
    emitted_events: int,
    last_processed_ts: Optional[dt.datetime],
) -> Dict[str, Any]:
    sym = str(symbol or "").upper()
    engine_candles = deepcopy((engine.candles or {}).get(sym, {}))
    engine_latest_ts = deepcopy((engine.latest_ts or {}).get(sym, {}))
    return {
        "snapshot_version": 1,
        "saved_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "symbol": sym,
        "run_id": str(run_id or ""),
        "seed_date": str(seed_date or ""),
        "sim_period": int(sim_period or 0),
        "last_processed_ts": _to_iso_utc(last_processed_ts),
        "first_live_ts": deepcopy(first_live_ts or {}),
        "last_live_ts": deepcopy(last_live_ts or {}),
        "first_live_to_bot": _json_safe(deepcopy(first_live_to_bot)),
        "last_live_to_bot": _json_safe(deepcopy(last_live_to_bot)),
        "emitted_events": int(emitted_events or 0),
        "engine_state": {
            "candles": _json_safe(engine_candles),
            "latest_ts": _json_safe(engine_latest_ts),
            "sim_clock_ts": _to_iso_utc(getattr(engine, "sim_clock_ts", None)),
        },
        "bridge_state": export_bos_fvg_ltf_snapshot_for_symbol(sym),
    }


def _persist_local_snapshot(
    *,
    symbol: str,
    run_id: str,
    seed_date: str,
    sim_period: int,
    engine: CandleEngine,
    first_live_ts: Dict[str, str],
    last_live_ts: Dict[str, str],
    first_live_to_bot: Optional[Dict[str, Any]],
    last_live_to_bot: Optional[Dict[str, Any]],
    emitted_events: int,
    last_processed_ts: Optional[dt.datetime],
) -> None:
    payload = _build_worker_snapshot(
        symbol=symbol,
        run_id=run_id,
        seed_date=seed_date,
        sim_period=sim_period,
        engine=engine,
        first_live_ts=first_live_ts,
        last_live_ts=last_live_ts,
        first_live_to_bot=first_live_to_bot,
        last_live_to_bot=last_live_to_bot,
        emitted_events=emitted_events,
        last_processed_ts=last_processed_ts,
    )
    _write_snapshot_file(_snapshot_path(symbol), payload)


def _restore_from_local_snapshot(
    *,
    symbol: str,
    seed_date: str,
    sim_period: int,
    engine: CandleEngine,
) -> Optional[Dict[str, Any]]:
    payload = _load_local_snapshot(symbol)
    if not payload:
        return None
    if str(payload.get("symbol") or "").upper() != str(symbol or "").upper():
        return None
    if str(payload.get("seed_date") or "") != str(seed_date or ""):
        return None
    if int(payload.get("sim_period") or 0) != int(sim_period or 0):
        return None

    engine_state = payload.get("engine_state") if isinstance(payload.get("engine_state"), dict) else {}
    sym = str(symbol or "").upper()
    candles_state = _json_restore(engine_state.get("candles") or {})
    latest_ts_state = _json_restore(engine_state.get("latest_ts") or {})
    engine.candles.setdefault(sym, {})
    engine.latest_ts.setdefault(sym, {})
    if isinstance(candles_state, dict):
        engine.candles[sym] = candles_state
    if isinstance(latest_ts_state, dict):
        engine.latest_ts[sym] = latest_ts_state
    engine.sim_clock_ts = _parse_iso_dt(engine_state.get("sim_clock_ts"))

    bridge_state = payload.get("bridge_state")
    if isinstance(bridge_state, dict):
        restore_bos_fvg_ltf_snapshot_for_symbol(sym, bridge_state)

    restored = {
        "last_processed_ts": _parse_iso_dt(payload.get("last_processed_ts")),
        "first_live_ts": payload.get("first_live_ts") if isinstance(payload.get("first_live_ts"), dict) else {},
        "last_live_ts": payload.get("last_live_ts") if isinstance(payload.get("last_live_ts"), dict) else {},
        "first_live_to_bot": _json_restore(payload.get("first_live_to_bot")),
        "last_live_to_bot": _json_restore(payload.get("last_live_to_bot")),
        "emitted_events": int(payload.get("emitted_events") or 0),
        "restored_run_id": str(payload.get("run_id") or ""),
        "runtime_mode": get_bos_fvg_ltf_runtime_mode(),
    }
    return restored


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




def _to_snake_case_key(value: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip())
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.strip("_").lower()


def _coerce_summary_value(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (int, float, bool)):
        return raw
    s = str(raw).strip()
    if s == "":
        return ""
    low = s.lower()
    if low in {"true", "false"}:
        return low == "true"
    if re.fullmatch(r"[-+]?\d+", s):
        try:
            return int(s)
        except Exception:
            return s
    if re.fullmatch(r"[-+]?\d*\.\d+", s):
        try:
            return float(s)
        except Exception:
            return s
    return s


def _parse_final_summary_lines(log_file_path: Path) -> Dict[str, Any]:
    summaries: Dict[str, Any] = {}
    if not log_file_path.exists():
        return summaries

    with log_file_path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            if "FINAL SUMMARY |" not in line:
                continue

            prefix, tail = line.split("FINAL SUMMARY |", 1)
            strategy_tag = ""
            if "[" in prefix and "]" in prefix:
                try:
                    strategy_tag = prefix[prefix.rfind("[") + 1 : prefix.rfind("]")].strip()
                except Exception:
                    strategy_tag = ""

            parsed: Dict[str, Any] = {}
            for part in tail.split("|"):
                if "=" not in part:
                    continue
                k, v = part.split("=", 1)
                key = _to_snake_case_key(k)
                parsed[key] = _coerce_summary_value(v.strip())

            timeframe = str(parsed.pop("tf", parsed.pop("timeframe", "")) or "").strip()
            if not timeframe:
                continue

            tf_summary = summaries.get(timeframe)
            if not isinstance(tf_summary, dict):
                tf_summary = {}
                summaries[timeframe] = tf_summary

            if strategy_tag:
                tf_summary["strategy_tag"] = strategy_tag
            tf_summary.update(parsed)

    return summaries

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
            "live_runs",
            payload=_sanitize_live_runs_payload(full_payload),
            returning="minimal",
        )
    except httpx.HTTPStatusError as e:
        # Some deployments have a reduced schema and reject one or more JSON columns.
        # Retry with a strict minimal payload so run tracking is still created.
        body = e.response.text[:500] if e.response is not None else str(e)
        logger.warning("full live_runs insert failed (retrying minimal payload): %s", body)
        await _sb_insert(
            client,
            base_url,
            key,
            "live_runs",
            payload=_sanitize_live_runs_payload(fallback_payload),
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
    sanitized = _sanitize_live_runs_payload(body)
    await _sb_patch(
        client,
        base_url,
        key,
        "live_runs",
        params={"id": f"eq.{run_id}"},
        payload=sanitized,
        returning="minimal",
    )


async def _list_live_ticker_rows(client: httpx.AsyncClient) -> list[Dict[str, Any]]:
    base_url, key = _sb_env()
    rows = await _sb_select(
        client,
        base_url,
        key,
        "live_ticker",
        params={
            "select": "*",
            "order": "symbol.asc",
        },
    )
    out: list[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row2 = dict(row)
        row2["_symbol_db"] = str(row2.get("symbol") or "")
        out.append(row2)
    return out


async def _fetch_live_ticker_strategy_config(
    client: httpx.AsyncClient,
    *,
    symbol_db: str,
) -> Optional[Dict[str, Any]]:
    base_url, key = _sb_env()
    row = await _sb_select_one(
        client,
        base_url,
        key,
        "live_ticker",
        params={
            "select": "run_strategy,selected_strategy",
            "symbol": f"eq.{symbol_db}",
            "limit": "1",
        },
    )
    if not row:
        return None
    return {
        "run_strategy": bool(row.get("run_strategy")),
        "selected_strategy": str(row.get("selected_strategy") or "").strip() or None,
    }


async def _sync_master_strategies_table(client: httpx.AsyncClient) -> None:
    base_url, key = _sb_env()
    strategy_ids = [str(x).strip() for x in get_available_strategy_ids() if str(x).strip()]
    payload = [{"strategy_key": strategy_id, "is_enabled": True} for strategy_id in strategy_ids]

    endpoint = f"{base_url}/rest/v1/master_strategies"
    headers = _sb_headers(key)
    headers["Prefer"] = "resolution=merge-duplicates,return=minimal"

    if payload:
        r = await client.post(
            endpoint,
            headers=headers,
            params={"on_conflict": "strategy_key"},
            json=payload,
            timeout=30.0,
        )
        r.raise_for_status()

        csv_values = ",".join(f'"{strategy_id}"' for strategy_id in strategy_ids)
        await _sb_delete(
            client,
            base_url,
            key,
            "master_strategies",
            params={"strategy_key": f"not.in.({csv_values})"},
            returning="minimal",
        )
    else:
        await _sb_delete(
            client,
            base_url,
            key,
            "master_strategies",
            params={},
            returning="minimal",
        )


def _apply_runtime_strategy_config(bot: IndicatorBot, symbol: str, config: Optional[Dict[str, Any]]) -> None:
    cfg = config or {}
    bot.set_strategy_runtime(
        symbol=symbol,
        run_strategy=cfg.get("run_strategy", False),
        selected_strategy=cfg.get("selected_strategy"),
    )


async def _start_job_row(client: httpx.AsyncClient, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Mark a live_ticker row as running and assign a fresh run_id,
    but DO NOT change start_sim. It remains the desired on/off switch.
    """
    base_url, key = _sb_env()
    symbol_db = str(row.get("_symbol_db") or row.get("symbol") or "")
    if not symbol_db:
        return None
    run_id = str(uuid.uuid4())
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    updated = await _sb_patch(
        client,
        base_url,
        key,
        "live_ticker",
        params={"symbol": f"eq.{symbol_db}"},
        payload={
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
            logger.warning("start patch matched zero live_ticker rows for symbol=%s", symbol_db)
            return None
        out = dict(updated[0])
    elif isinstance(updated, dict):
        out = dict(updated)
    else:
        logger.warning("start patch returned unexpected payload type=%s for symbol=%s", type(updated).__name__, symbol_db)
        return None

    out["_symbol_db"] = symbol_db
    out["run_id"] = str(out.get("run_id") or run_id)
    return out


async def _fetch_claimed_job(
    client: httpx.AsyncClient,
    *,
    symbol_db: str,
    run_id: str,
) -> Optional[Dict[str, Any]]:
    base_url, key = _sb_env()
    row = await _sb_select_one(
        client,
        base_url,
        key,
        "live_ticker",
        params={
            "select": "*",
            "symbol": f"eq.{symbol_db}",
            "run_id": f"eq.{run_id}",
            "status": "eq.running",
            "limit": "1",
        },
    )
    if not row:
        return None
    out = dict(row)
    out["_symbol_db"] = symbol_db
    out["run_id"] = run_id
    return out


def _parallel_workers_from_env(job_count: int) -> int:
    """
    Resolve worker concurrency for running claimed jobs.

    If SIM_PARALLEL_WORKERS is not set, default to the number of claimed jobs so
    multiple live_ticker rows run in parallel without extra configuration.
    """
    raw = os.getenv("SIM_PARALLEL_WORKERS")
    if raw is None or raw.strip() == "":
        return max(1, int(job_count))

    raw = raw.strip()
    try:
        value = int(raw)
    except Exception:
        logger.warning(
            "Invalid SIM_PARALLEL_WORKERS=%r; defaulting to claimed jobs=%d",
            raw,
            job_count,
        )
        return max(1, int(job_count))
    return max(1, value)


async def _mark_done(client: httpx.AsyncClient, symbol: str) -> None:
    base_url, key = _sb_env()
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    await _sb_patch(
        client,
        base_url,
        key,
        "live_ticker",
        params={"symbol": f"eq.{symbol}"},
        payload={"status": "done", "finished_at": now},
        returning="minimal",
    )


async def _mark_stopped(client: httpx.AsyncClient, symbol: str, note: Optional[str] = None) -> None:
    base_url, key = _sb_env()
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    payload: Dict[str, Any] = {
        "status": "stopped",
        "finished_at": now,
    }
    if note:
        payload["error_message"] = note[:2000]
    await _sb_patch(
        client,
        base_url,
        key,
        "live_ticker",
        params={"symbol": f"eq.{symbol}"},
        payload=payload,
        returning="minimal",
    )


async def _mark_error(client: httpx.AsyncClient, symbol: str, msg: str) -> None:
    base_url, key = _sb_env()
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    await _sb_patch(
        client,
        base_url,
        key,
        "live_ticker",
        params={"symbol": f"eq.{symbol}"},
        payload={"status": "error", "error_message": msg[:2000], "finished_at": now},
        returning="minimal",
    )


# ----------------------------- Worker Main -----------------------------

async def _run_claimed_job(client: httpx.AsyncClient, job: Dict[str, Any]) -> int:
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
        await _create_simulation_run(
            client=client,
            run_id=run_id,
            symbol=symbol,
            seed_date=str(seed_date),
            sim_period=sim_period,
        )
    except Exception as e:
        logger.warning("failed to create live_runs row for run_id=%s: %s", run_id, e)

    try:
        with _capture_stdout_to_file(log_local_path):
            print(f"[SIM_LOG] local log capture path={log_local_path.as_posix()}")
            print(f"[SIM_LOG] storage target path=Simulation_runs/{log_storage_path}")
            # Candle engine reads candles from DB via Supabase REST (we’ll patch candle_engine next)
            engine = CandleEngine(symbols=[symbol])

            # Indicator bot in simulation mode (no DB writes)
            bot = IndicatorBot(engine=engine, sim_mode=True)
            _apply_runtime_strategy_config(bot, symbol, job)

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

            first_live_ts: Dict[str, str] = {}
            last_live_ts: Dict[str, str] = {}
            first_live_to_bot: Optional[Dict[str, Any]] = None
            last_live_to_bot: Optional[Dict[str, Any]] = None
            emitted_events = 0
            poll_sleep_s = _poll_sleep_seconds()
            last_processed_ts = engine.latest_ts.get(symbol, {}).get("1m")
            restored_snapshot = _restore_from_local_snapshot(
                symbol=symbol,
                seed_date=str(seed_date),
                sim_period=sim_period,
                engine=engine,
            )
            if restored_snapshot:
                first_live_ts = dict(restored_snapshot.get("first_live_ts") or {})
                last_live_ts = dict(restored_snapshot.get("last_live_ts") or {})
                first_live_to_bot = restored_snapshot.get("first_live_to_bot")
                last_live_to_bot = restored_snapshot.get("last_live_to_bot")
                emitted_events = int(restored_snapshot.get("emitted_events") or 0)
                last_processed_ts = restored_snapshot.get("last_processed_ts") or last_processed_ts
                runtime_mode = restored_snapshot.get("runtime_mode") if isinstance(restored_snapshot.get("runtime_mode"), dict) else {}
                print(
                    f"[SIM_WORKER] snapshot_restore | Symbol={symbol} | "
                    f"snapshot_run_id={restored_snapshot.get('restored_run_id')} | "
                    f"resume_last_ts={_ts_str(last_processed_ts)} | "
                    f"execution_enabled={runtime_mode.get('execution_enabled')} | "
                    f"live_mode={runtime_mode.get('live_mode')}"
                )

            # ------------------------------------------------------------
            # Phase 1: catch-up
            # - track indicators / setups
            # - DO NOT materialize bridge rows
            # - DO NOT sync bridge rows to DB
            # ------------------------------------------------------------
            catchup_batches = 0
            catchup_rows_total = 0
            restored_runtime = get_bos_fvg_ltf_runtime_mode() if restored_snapshot else {}
            resume_direct_live = bool(restored_snapshot) and bool(restored_runtime.get("execution_enabled")) and bool(restored_runtime.get("live_mode"))
            if not resume_direct_live:
                set_bos_fvg_ltf_runtime_mode(execution_enabled=False, live_mode=False)
                print(
                    f"[SIM_WORKER] catchup_start | Symbol={symbol} | "
                    f"seed_last_ts={_ts_str(last_processed_ts)} | mode=drain_until_empty"
                )
                while True:
                    catchup_rows = await _fetch_new_rth_1m_rows(
                        engine=engine,
                        symbol=symbol,
                        after_ts=last_processed_ts,
                        upto_ts=None,
                        limit=5000,
                    )
                    if not catchup_rows:
                        break

                    catchup_batches += 1
                    catchup_rows_total += len(catchup_rows)
                    first_batch_ts = _ts_str((catchup_rows[0] or {}).get("ts")) if catchup_rows else None
                    last_batch_ts = _ts_str((catchup_rows[-1] or {}).get("ts")) if catchup_rows else None
                    print(
                        f"[SIM_WORKER] catchup_batch | Symbol={symbol} | "
                        f"batch={catchup_batches} | rows={len(catchup_rows)} | "
                        f"first_ts={first_batch_ts} | last_ts={last_batch_ts}"
                    )

                    processed_ts, first_live_to_bot, last_live_to_bot, processed_events = await _process_rth_1m_rows(
                        engine=engine,
                        bot=bot,
                        client=client,
                        symbol=symbol,
                        rows_1m=catchup_rows,
                        bridge_sync_enabled=False,
                        first_live_ts=first_live_ts,
                        last_live_ts=last_live_ts,
                        first_live_to_bot=first_live_to_bot,
                        last_live_to_bot=last_live_to_bot,
                    )
                    emitted_events += processed_events
                    if processed_ts is None:
                        break
                    last_processed_ts = processed_ts
                    _persist_local_snapshot(
                        symbol=symbol,
                        run_id=run_id,
                        seed_date=str(seed_date),
                        sim_period=sim_period,
                        engine=engine,
                        first_live_ts=first_live_ts,
                        last_live_ts=last_live_ts,
                        first_live_to_bot=first_live_to_bot,
                        last_live_to_bot=last_live_to_bot,
                        emitted_events=emitted_events,
                        last_processed_ts=last_processed_ts,
                    )

                print(
                    f"[SIM_WORKER] catchup_done | Symbol={symbol} | "
                    f"last_processed_ts={_ts_str(last_processed_ts)} | "
                    f"batches={catchup_batches} | rows_total={catchup_rows_total}"
                )
            else:
                print(
                    f"[SIM_WORKER] catchup_skipped_snapshot_resume | Symbol={symbol} | "
                    f"resume_last_ts={_ts_str(last_processed_ts)}"
                )

            # ------------------------------------------------------------
            # Phase 2: continuous live loop
            # - preserve setup state built during catch-up
            # - allow first live candle to materialize pending setup
            # - sync bridge rows to DB
            # ------------------------------------------------------------
            set_bos_fvg_ltf_runtime_mode(execution_enabled=True, live_mode=True)
            live_start_anchor_ts = last_processed_ts
            print(
                f"[SIM_WORKER] live_loop_start | Symbol={symbol} | "
                f"after_ts={_ts_str(live_start_anchor_ts)} | poll_s={poll_sleep_s}"
            )

            while True:
                if not is_rth_now():
                    await asyncio.sleep(max(5.0, poll_sleep_s))
                    continue

                latest_strategy_cfg = await _fetch_live_ticker_strategy_config(
                    client,
                    symbol_db=symbol_db,
                )
                _apply_runtime_strategy_config(bot, symbol, latest_strategy_cfg)

                live_rows = await _fetch_new_rth_1m_rows(
                    engine=engine,
                    symbol=symbol,
                    after_ts=last_processed_ts,
                    upto_ts=None,
                    limit=5000,
                )
                if not live_rows:
                    await asyncio.sleep(poll_sleep_s)
                    continue

                processed_ts, first_live_to_bot, last_live_to_bot, processed_events = await _process_rth_1m_rows(
                    engine=engine,
                    bot=bot,
                    client=client,
                    symbol=symbol,
                    rows_1m=live_rows,
                    bridge_sync_enabled=True,
                    first_live_ts=first_live_ts,
                    last_live_ts=last_live_ts,
                    first_live_to_bot=first_live_to_bot,
                    last_live_to_bot=last_live_to_bot,
                )
                emitted_events += processed_events
                if processed_ts is not None:
                    last_processed_ts = processed_ts
                    _persist_local_snapshot(
                        symbol=symbol,
                        run_id=run_id,
                        seed_date=str(seed_date),
                        sim_period=sim_period,
                        engine=engine,
                        first_live_ts=first_live_ts,
                        last_live_ts=last_live_ts,
                        first_live_to_bot=first_live_to_bot,
                        last_live_to_bot=last_live_to_bot,
                        emitted_events=emitted_events,
                        last_processed_ts=last_processed_ts,
                    )
                    try:
                        await _update_simulation_run(
                            client,
                            run_id,
                            payload={
                                "status": "running",
                                "simulation_start_time": _to_iso_utc((first_live_to_bot or {}).get("ts")),
                                "simulation_end_time": _to_iso_utc((last_live_to_bot or {}).get("ts")),
                                "config": {
                                    "symbol": symbol,
                                    "seed_date": seed_date,
                                    "sim_period": sim_period,
                                    "seed_counts": seed_counts,
                                    "mode": "catchup_then_live_poll",
                                    "last_processed_ts": _to_iso_utc(last_processed_ts),
                                },
                            },
                        )
                    except Exception as hb_err:
                        logger.warning("failed to update running heartbeat for %s: %s", symbol, hb_err)

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

            # Print internal-sim BOS/FVG summaries once at the end of the simulation.
            # Bridge-only module does not emit final trade summaries.
            try:
                print_bos_fvg_ltf_sim_final_summaries()
            except Exception as e:
                logger.warning("failed to print BOS_FVG final summaries: %s", e)
            print(f"[SIM_LOG] local log file complete path={log_local_path.as_posix()}")

    except asyncio.CancelledError:
        logger.info("simulation cancelled for symbol=%s", symbol)
        msg = "cancelled"
        try:
            event_counters, event_candles = _build_event_payloads()
            parsed_tf_summaries = _parse_final_summary_lines(log_local_path)
            base_trades_summary, trades = _build_trades_payload(bot, symbol)
            trades_summary_payload: Dict[str, Any] = {}
            if parsed_tf_summaries:
                trades_summary_payload.update(parsed_tf_summaries)
            elif base_trades_summary:
                trades_summary_payload["multi"] = dict(base_trades_summary)
            await _update_simulation_run(
                client,
                run_id,
                payload={
                    "symbol": symbol,
                    "strategy_name": "SPY_VWAP_Pullback_Scalp_Sim",
                    "strategy_version": "v1.0",
                    "status": "done",
                    "end_time": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "simulation_start_time": _to_iso_utc((first_live_to_bot or {}).get("ts")),
                    "simulation_end_time": _to_iso_utc((last_live_to_bot or {}).get("ts")),
                    "event_counters": event_counters,
                    "event_candles": event_candles,
                    "trades_summary": trades_summary_payload,
                    "trades": trades,
                    "config": {
                        "symbol": symbol,
                        "seed_date": seed_date,
                        "sim_period": sim_period,
                        "seed_counts": seed_counts,
                        "mode": "catchup_then_live_poll",
                    },
                    "error_message": None,
                },
            )
            await _mark_done(client, symbol_db)
            _delete_local_snapshot(symbol)
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
        except Exception as finalize_err:
            logger.warning("finalize after cancellation failed for %s: %s", symbol, finalize_err)
        raise

    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        logger.exception("simulation failed for symbol=%s: %s", symbol, msg)
        try:
            set_bos_fvg_ltf_runtime_mode(execution_enabled=True, live_mode=True)
        except Exception:
            pass
        try:
            await _update_simulation_run(
                client,
                run_id,
                payload={
                    "symbol": symbol,
                    "strategy_name": "SPY_VWAP_Pullback_Scalp_Sim",
                    "strategy_version": "v1.0",
                    "status": "error",
                    "end_time": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "simulation_start_time": None,
                    "simulation_end_time": None,
                    "error_message": msg[:2000],
                    "config": {
                        "symbol": symbol,
                        "seed_date": seed_date,
                        "sim_period": sim_period,
                    },
                },
            )
        except Exception as e3:
            logger.warning("failed to update live_runs error payload run_id=%s: %s", run_id, e3)
        try:
            await _mark_error(client, symbol_db or symbol, msg)
        except Exception as e2:
            logger.exception("failed to mark DB error for symbol=%s: %s", symbol, e2)
        return 1


async def _run_job_in_subprocess(job: Dict[str, Any]) -> int:
    symbol_db = str(job.get("_symbol_db") or job.get("symbol") or "")
    run_id = str(job.get("run_id") or "")
    env = os.environ.copy()
    env["SIM_CHILD_RUN"] = "1"
    env["SIM_CHILD_SYMBOL"] = symbol_db
    env["SIM_CHILD_RUN_ID"] = run_id
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        __file__,
        env=env,
    )
    return await proc.wait()


async def _spawn_job_subprocess(job: Dict[str, Any]) -> asyncio.subprocess.Process:
    symbol_db = str(job.get("_symbol_db") or job.get("symbol") or "")
    run_id = str(job.get("run_id") or "")
    env = os.environ.copy()
    env["SIM_CHILD_RUN"] = "1"
    env["SIM_CHILD_SYMBOL"] = symbol_db
    env["SIM_CHILD_RUN_ID"] = run_id
    return await asyncio.create_subprocess_exec(
        sys.executable,
        __file__,
        env=env,
    )


async def _stop_child_process(proc: asyncio.subprocess.Process, *, timeout_s: float = 10.0) -> None:
    if proc.returncode is not None:
        return
    try:
        proc.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=timeout_s)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            return
        await proc.wait()


async def main() -> int:
    logger.info("sim worker booting")
    # quick env validation early
    try:
        _sb_env()
    except Exception as e:
        logger.exception("supabase env validation failed: %s", e)
        return 1

    async with httpx.AsyncClient() as client:
        # Child mode: run one specific already-claimed job.
        if (os.getenv("SIM_CHILD_RUN") or "").strip().lower() == "1":
            child_symbol = str(os.getenv("SIM_CHILD_SYMBOL") or "").strip()
            child_run_id = str(os.getenv("SIM_CHILD_RUN_ID") or "").strip()
            if not child_symbol or not child_run_id:
                logger.error("SIM_CHILD_RUN=1 requires SIM_CHILD_SYMBOL and SIM_CHILD_RUN_ID")
                return 1
            claimed = await _fetch_claimed_job(client, symbol_db=child_symbol, run_id=child_run_id)
            if not claimed:
                logger.error("claimed job not found symbol=%s run_id=%s", child_symbol, child_run_id)
                return 1
            return await _run_claimed_job(client, claimed)
        try:
            await _sync_master_strategies_table(client)
        except Exception as e:
            print(f"[SIM_WORKER][STARTUP_SYNC_ERROR] {type(e).__name__}: {e}")


        poll_s = _supervisor_poll_seconds()
        running_children: Dict[str, Dict[str, Any]] = {}
        logger.info("sim supervisor started; poll_s=%s", poll_s)

        while True:
            try:
                rows = await _list_live_ticker_rows(client)
            except Exception as e:
                logger.warning("failed to list live_ticker rows: %s", e)
                await asyncio.sleep(poll_s)
                continue

            rows_by_symbol: Dict[str, Dict[str, Any]] = {
                str(r.get("_symbol_db") or r.get("symbol") or ""): r
                for r in rows
                if str(r.get("_symbol_db") or r.get("symbol") or "")
            }

            # Start rows that want to run and do not already have a child.
            for symbol_db, row in rows_by_symbol.items():
                desired_on = str(row.get("start_sim") or "").strip().lower() == "y"
                child_info = running_children.get(symbol_db)
                if desired_on and child_info is None:
                    try:
                        started = await _start_job_row(client, row)
                        if not started:
                            continue
                        proc = await _spawn_job_subprocess(started)
                        running_children[symbol_db] = {
                            "proc": proc,
                            "run_id": started.get("run_id"),
                        }
                        logger.info(
                            "spawned child for symbol=%s run_id=%s pid=%s",
                            symbol_db,
                            started.get("run_id"),
                            getattr(proc, "pid", None),
                        )
                    except Exception as e:
                        logger.exception("failed to spawn child for symbol=%s: %s", symbol_db, e)
                        try:
                            await _mark_error(client, symbol_db, f"spawn failed: {type(e).__name__}: {e}")
                        except Exception:
                            pass

            # Stop children whose rows no longer want to run, or whose row disappeared.
            for symbol_db, info in list(running_children.items()):
                row = rows_by_symbol.get(symbol_db)
                desired_on = bool(row) and str(row.get("start_sim") or "").strip().lower() == "y"
                if not desired_on:
                    proc = info.get("proc")
                    if proc is not None:
                        try:
                            await _stop_child_process(proc)
                        except Exception as e:
                            logger.warning("failed to stop child for symbol=%s: %s", symbol_db, e)
                    try:
                        await _mark_stopped(client, symbol_db, note="start_sim turned off")
                    except Exception as e:
                        logger.warning("failed to mark stopped for symbol=%s: %s", symbol_db, e)
                    running_children.pop(symbol_db, None)
                    logger.info("stopped child for symbol=%s because start_sim is not 'y'", symbol_db)

            # Respawn children that died unexpectedly while the row still wants to run.
            for symbol_db, info in list(running_children.items()):
                proc = info.get("proc")
                if proc is None:
                    continue
                rc = proc.returncode
                if rc is None:
                    continue

                row = rows_by_symbol.get(symbol_db)
                desired_on = bool(row) and str(row.get("start_sim") or "").strip().lower() == "y"
                logger.info("child exited for symbol=%s rc=%s desired_on=%s", symbol_db, rc, desired_on)
                running_children.pop(symbol_db, None)

                if desired_on and row is not None:
                    try:
                        started = await _start_job_row(client, row)
                        if not started:
                            continue
                        proc2 = await _spawn_job_subprocess(started)
                        running_children[symbol_db] = {
                            "proc": proc2,
                            "run_id": started.get("run_id"),
                        }
                        logger.info(
                            "respawned child for symbol=%s run_id=%s pid=%s",
                            symbol_db,
                            started.get("run_id"),
                            getattr(proc2, "pid", None),
                        )
                    except Exception as e:
                        logger.exception("failed to respawn child for symbol=%s: %s", symbol_db, e)
                        try:
                            await _mark_error(client, symbol_db, f"respawn failed: {type(e).__name__}: {e}")
                        except Exception:
                            pass

            await asyncio.sleep(poll_s)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
