import os
import asyncio
import datetime as dt
import uuid
import logging
import io
import sys
import json
from pathlib import Path
from contextlib import contextmanager
from typing import Any, Dict, Optional
import re

import httpx

from candle_engine import CandleEngine
from indicator_bot import IndicatorBot
from liquidity_pool_builder import print_last_liquidity_output
from strategy_bos_fvg import print_bos_fvg_final_summaries as print_bos_fvg_htf_final_summaries
from strategy_bos_fvg_ltf import (
    print_bos_fvg_final_summaries as print_bos_fvg_ltf_final_summaries,
    get_live_bridge_rows as get_bos_fvg_ltf_live_bridge_rows,
    apply_live_bridge_db_state as apply_bos_fvg_ltf_live_bridge_db_state,
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


def _sanitize_simulation_runs_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only columns that exist in simulation_runs to avoid PostgREST 400 errors."""
    return {k: v for k, v in payload.items() if k in SIMULATION_RUNS_ALLOWED_COLUMNS}



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
            parsed = parsed.replace(tzinfo=dt.ZoneInfo("America/New_York"))
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
    if payload.get("end_time_et") is not None:
        end_time = _et_naive_to_utc_iso(payload.get("end_time_et"))
        if end_time is not None:
            out["end_time"] = end_time
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
            active_patch_payload = _sanitize_bridge_active_trades_payload(
                {
                    "manage": row.get("manage"),
                    "sl_level": row.get("sl_level"),
                    "note": f"bridge:{setup_id}",
                }
            )
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
            "simulation_runs",
            payload=_sanitize_simulation_runs_payload(full_payload),
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
            payload=_sanitize_simulation_runs_payload(fallback_payload),
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
    sanitized = _sanitize_simulation_runs_payload(body)
    await _sb_patch(
        client,
        base_url,
        key,
        "simulation_runs",
        params={"id": f"eq.{run_id}"},
        payload=sanitized,
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
        "sim_ticker",
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
    multiple sim_ticker rows run in parallel without extra configuration.
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
        logger.warning("failed to create simulation_runs row for run_id=%s: %s", run_id, e)

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
                    try:
                        await _sync_bos_fvg_bridge_rows_to_supabase(client)
                    except Exception as bridge_err:
                        logger.warning("bridge sync failed during live loop: %s", bridge_err)
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

        event_counters, event_candles = _build_event_payloads()
        parsed_tf_summaries = _parse_final_summary_lines(log_local_path)
        base_trades_summary, trades = _build_trades_payload(bot, symbol)
        trades_summary_payload: Dict[str, Any] = {}
        if parsed_tf_summaries:
            trades_summary_payload.update(parsed_tf_summaries)
        elif base_trades_summary:
            trades_summary_payload["multi"] = dict(base_trades_summary)

        sim_start_ts = _to_iso_utc((first_live_to_bot or {}).get("ts"))
        sim_end_ts = _to_iso_utc((last_live_to_bot or {}).get("ts"))
        end_time = dt.datetime.now(dt.timezone.utc).isoformat()
        try:
            await _sync_bos_fvg_bridge_rows_to_supabase(client)
        except Exception as bridge_err:
            logger.warning("failed to sync BOS_FVG bridge rows to Supabase: %s", bridge_err)
        await _update_simulation_run(
            client,
            run_id,
            payload={
                "symbol": symbol,
                "strategy_name": "SPY_VWAP_Pullback_Scalp_Sim",
                "strategy_version": "v1.0",
                "status": "done",
                "end_time": end_time,
                "simulation_start_time": sim_start_ts,
                "simulation_end_time": sim_end_ts,
                "event_counters": event_counters,
                "event_candles": event_candles,
                "trades_summary": trades_summary_payload,
                "trades": trades,
                "config": {
                    "symbol": symbol,
                    "seed_date": seed_date,
                    "sim_period": sim_period,
                    "seed_counts": seed_counts,
                },
                "error_message": None,
            },
        )
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
            logger.warning("failed to update simulation_runs error payload run_id=%s: %s", run_id, e3)
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

        jobs: list[Dict[str, Any]] = []
        while True:
            job = await _claim_one_job(client)
            if not job:
                break
            jobs.append(job)

        if not jobs:
            logger.info("no sim_ticker rows with start_sim='y'; worker exiting")
            return 0

        max_parallel = _parallel_workers_from_env(len(jobs))
        logger.info("claimed %d simulation jobs; parallel_workers=%d", len(jobs), max_parallel)

        if max_parallel == 1 or len(jobs) == 1:
            result = 0
            for j in jobs:
                rc = await _run_claimed_job(client, j)
                if rc != 0:
                    result = rc
            return result

        semaphore = asyncio.Semaphore(max_parallel)

        async def _run_limited(j: Dict[str, Any]) -> int:
            async with semaphore:
                return await _run_job_in_subprocess(j)

        results = await asyncio.gather(*[_run_limited(j) for j in jobs], return_exceptions=False)
        return 0 if all(int(rc) == 0 for rc in results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
