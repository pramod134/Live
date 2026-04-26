import os
import json
import datetime as dt
from typing import Any

import httpx


def _db_env() -> tuple[str, str]:
    base_url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
        or ""
    )
    if not base_url or not key:
        raise RuntimeError(
            "Missing SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY)."
        )
    return base_url, key


def db_insert_raw(
    *,
    table: str,
    payload: Any,
    returning: str = "minimal",
    log_label: str = "DB",
) -> dict:
    """
    Generic raw insert helper.

    Rules:
    - does not inspect table semantics
    - does not inspect field names
    - does not query before insert
    - does not query after insert
    - sends exactly what it receives
    - waits for DB success/failure
    - logs and returns
    - retries up to 2 additional times on failure (total 3 attempts)
    - never raises; always returns success/failure payload
    """
    base_url, key = _db_env()
    endpoint = f"{base_url.rstrip('/')}/rest/v1/{table}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": f"return={returning}",
    }

    row_count = len(payload) if isinstance(payload, list) else 1

    print(
        f"[{log_label}][DB_WRITE] action=insert table={table} rows={row_count} "
        f"payload={json.dumps(payload, default=str, sort_keys=True)}"
    )

    last_error_payload = None

    for attempt in range(1, 4):
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.post(endpoint, headers=headers, json=payload)

            response.raise_for_status()

            print(
                f"[{log_label}][DB_APPLIED] action=insert table={table} rows={row_count} attempt={attempt}"
            )

            return {
                "success": True,
                "status_code": response.status_code,
                "attempts": attempt,
                "data": response.json() if response.text else None,
            }

        except httpx.HTTPStatusError as e:
            response = e.response
            try:
                error_body: Any = response.json() if response is not None and response.text else None
            except Exception:
                error_body = response.text if response is not None else str(e)

            last_error_payload = {
                "success": False,
                "status_code": response.status_code if response is not None else None,
                "attempts": attempt,
                "error": error_body,
            }

            print(
                f"[{log_label}][DB_ERROR] action=insert table={table} rows={row_count} "
                f"attempt={attempt} status={response.status_code if response is not None else 'unknown'} "
                f"error={json.dumps(error_body, default=str, sort_keys=True)}"
            )

        except Exception as e:
            last_error_payload = {
                "success": False,
                "status_code": None,
                "attempts": attempt,
                "error": str(e),
            }

            print(
                f"[{log_label}][DB_ERROR] action=insert table={table} rows={row_count} "
                f"attempt={attempt} error={str(e)}"
            )

    return last_error_payload or {
        "success": False,
        "status_code": None,
        "attempts": 3,
        "error": "unknown insert failure",
    }


def db_upsert_skinny_snapshot(
    *,
    symbol: str,
    mode: str,
    snapshot: dict,
    log_label: str = "SKINNY_SNAPSHOT",
    returning: str = "minimal",
) -> dict:
    """
    Upsert one combined symbol-level skinny snapshot.

    Expected table:
      public.skinny_snapshots

    Conflict key:
      (symbol, mode)
    """
    if not isinstance(snapshot, dict):
        return {
            "success": False,
            "status_code": None,
            "error": "snapshot must be a dict",
        }

    sym = str(symbol or "").upper().strip()
    write_mode = str(mode or "").strip()
    if not sym or not write_mode:
        return {
            "success": False,
            "status_code": None,
            "error": "symbol and mode are required",
        }

    base_url, key = _db_env()
    endpoint = f"{base_url.rstrip('/')}/rest/v1/skinny_snapshots"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": f"resolution=merge-duplicates,return={returning}",
    }
    params = {
        "on_conflict": "symbol,mode",
    }
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    payload = {
        "symbol": sym,
        "mode": write_mode,
        "asof": now_iso,
        "snapshot": snapshot,
        "created_at": now_iso,
    }

    print(
        f"[{log_label}][DB_WRITE] action=upsert table=skinny_snapshots "
        f"symbol={sym} mode={write_mode}"
    )

    last_error_payload = None
    for attempt in range(1, 4):
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.post(
                    endpoint,
                    headers=headers,
                    params=params,
                    json=payload,
                )

            response.raise_for_status()

            print(
                f"[{log_label}][DB_APPLIED] action=upsert table=skinny_snapshots "
                f"symbol={sym} mode={write_mode} attempt={attempt}"
            )

            return {
                "success": True,
                "status_code": response.status_code,
                "attempts": attempt,
                "data": response.json() if response.text else None,
                "error": None,
            }

        except httpx.HTTPStatusError as e:
            response = e.response
            try:
                error_body: Any = response.json() if response is not None and response.text else None
            except Exception:
                error_body = response.text if response is not None else str(e)

            last_error_payload = {
                "success": False,
                "status_code": response.status_code if response is not None else None,
                "attempts": attempt,
                "error": error_body,
            }

            print(
                f"[{log_label}][DB_ERROR] action=upsert table=skinny_snapshots "
                f"symbol={sym} mode={write_mode} attempt={attempt} "
                f"status={response.status_code if response is not None else 'unknown'} "
                f"error={json.dumps(error_body, default=str, sort_keys=True)}"
            )

        except Exception as e:
            last_error_payload = {
                "success": False,
                "status_code": None,
                "attempts": attempt,
                "error": str(e),
            }
            print(
                f"[{log_label}][DB_ERROR] action=upsert table=skinny_snapshots "
                f"symbol={sym} mode={write_mode} attempt={attempt} error={str(e)}"
            )

    return last_error_payload or {
        "success": False,
        "status_code": None,
        "attempts": 3,
        "error": "unknown skinny snapshot upsert failure",
    }


def db_insert_raw_trade_ideas(
    *,
    snapshot_id: str | None,
    trade_ideas: list[dict],
    log_label: str = "RAW_TRADE_IDEAS",
) -> dict:
    """
    Insert raw trade_finder output into public.raw_trade_ideas.

    Rules:
    - stores exactly what trade_finder emitted
    - no dedupe
    - no filtering
    - no activation
    - no execution
    """
    rows = []
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()

    for trade in trade_ideas or []:
        if not isinstance(trade, dict):
            continue

        rows.append(
            {
                "snapshot_id": snapshot_id,
                "symbol": trade.get("symbol"),
                "horizon": trade.get("horizon"),
                "tf": trade.get("tf"),
                "trade_type": trade.get("trade_type"),
                "strategy": trade.get("strategy"),
                "direction": trade.get("direction"),
                "score": trade.get("score"),
                "confidence": trade.get("confidence"),
                "risk_tier": trade.get("risk_tier"),
                "rr_to_tp1": trade.get("rr_to_tp1"),
                "entry_zone_json": trade.get("entry_zone") or {},
                "stop_json": trade.get("stop") or {},
                "targets_json": trade.get("targets") or [],
                "tags_json": trade.get("tags") or [],
                "warnings_json": trade.get("warnings") or [],
                "source_tfs_json": trade.get("source_tfs") or [],
                "reason": trade.get("reason"),
                "raw_json": trade,
                "created_at": now_iso,
            }
        )

    if not rows:
        return {
            "success": True,
            "status_code": None,
            "attempts": 0,
            "data": None,
            "error": None,
            "rows": 0,
        }

    return db_insert_raw(
        table="raw_trade_ideas",
        payload=rows,
        returning="minimal",
        log_label=log_label,
    )


def active_trades_checker(
    *,
    strategy: str,
    version: str,
    setup_id: str,
    log_label: str = "DB",
) -> dict:
    base_url, key = _db_env()
    endpoint = f"{base_url.rstrip('/')}/rest/v1/active_trades"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    params = {
        "select": "id,tags,status,manage,qty",
        "tags": f'cs.{{"strategy:{strategy}","version:{version}","id:{setup_id}"}}',
    }

    print(
        f"[{log_label}][DB_READ] action=select table=active_trades "
        f"strategy={strategy} version={version} setup_id={setup_id} "
        f"params={json.dumps(params, default=str, sort_keys=True)}"
    )

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(endpoint, headers=headers, params=params)

        response.raise_for_status()

        rows = response.json()
        rows = rows if isinstance(rows, list) else []

        managing_rows = [
            row for row in rows
            if (
                str(row.get("manage") or "") == "O"
                or (
                    str(row.get("status") or "") == "nt-managing"
                    and str(row.get("manage") or "") == "Y"
                )
            )
        ]

        waiting_rows = [
            row for row in rows
            if (
                str(row.get("status") or "") == "nt-waiting"
                and str(row.get("manage") or "") == "Y"
            )
        ]

        result = {
            "success": True,
            "managing_present": len(managing_rows) > 0,
            "managing_qty": len(managing_rows),
            "waiting_present": len(waiting_rows) > 0,
            "waiting_qty": len(waiting_rows),
            "rows_found": len(rows),
            "data": rows,
            "error": None,
        }

        print(
            f"[{log_label}][DB_RESULT] action=select table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id} "
            f"rows_found={result['rows_found']} "
            f"managing_present={result['managing_present']} managing_qty={result['managing_qty']} "
            f"waiting_present={result['waiting_present']} waiting_qty={result['waiting_qty']}"
        )

        return result

    except httpx.HTTPStatusError as e:
        response = e.response
        try:
            error_body: Any = response.json() if response is not None and response.text else None
        except Exception:
            error_body = response.text if response is not None else str(e)

        print(
            f"[{log_label}][DB_ERROR] action=select table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id} "
            f"status={response.status_code if response is not None else 'unknown'} "
            f"error={json.dumps(error_body, default=str, sort_keys=True)}"
        )

        return {
            "success": False,
            "managing_present": False,
            "managing_qty": 0,
            "waiting_present": False,
            "waiting_qty": 0,
            "rows_found": 0,
            "data": None,
            "error": error_body,
        }

    except Exception as e:
        print(
            f"[{log_label}][DB_ERROR] action=select table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id} "
            f"error={str(e)}"
        )

        return {
            "success": False,
            "managing_present": False,
            "managing_qty": 0,
            "waiting_present": False,
            "waiting_qty": 0,
            "rows_found": 0,
            "data": None,
            "error": str(e),
        }


def active_cleanup(
    *,
    strategy: str,
    version: str,
    setup_id: str,
    log_label: str = "DB",
    returning: str = "minimal",
) -> dict:
    base_url, key = _db_env()
    endpoint = f"{base_url.rstrip('/')}/rest/v1/active_trades"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": f"return={returning}",
    }
    params = {
        "tags": f'cs.{{"strategy:{strategy}","version:{version}","id:{setup_id}"}}',
    }
    payload = {
        "manage": "C",
        "note": f"bridge:{setup_id}",
    }

    print(
        f"[{log_label}][DB_WRITE] action=patch table=active_trades "
        f"strategy={strategy} version={version} setup_id={setup_id} "
        f"params={json.dumps(params, default=str, sort_keys=True)} "
        f"payload={json.dumps(payload, default=str, sort_keys=True)}"
    )

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.patch(endpoint, headers=headers, params=params, json=payload)

        response.raise_for_status()

        print(
            f"[{log_label}][DB_APPLIED] action=patch table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id}"
        )

        return {
            "success": True,
            "status_code": response.status_code,
            "data": response.json() if response.text else None,
            "error": None,
        }

    except httpx.HTTPStatusError as e:
        response = e.response
        try:
            error_body: Any = response.json() if response is not None and response.text else None
        except Exception:
            error_body = response.text if response is not None else str(e)

        print(
            f"[{log_label}][DB_ERROR] action=patch table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id} "
            f"status={response.status_code if response is not None else 'unknown'} "
            f"error={json.dumps(error_body, default=str, sort_keys=True)}"
        )

        return {
            "success": False,
            "status_code": response.status_code if response is not None else None,
            "data": None,
            "error": error_body,
        }

    except Exception as e:
        print(
            f"[{log_label}][DB_ERROR] action=patch table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id} "
            f"error={str(e)}"
        )

        return {
            "success": False,
            "status_code": None,
            "data": None,
            "error": str(e),
        }


def db_updater(
    *,
    strategy: str,
    version: str,
    setup_id: str,
    leg: int | str,
    trade: int | str,
    sl_level: float,
    log_label: str = "DB",
    returning: str = "minimal",
) -> dict:
    base_url, key = _db_env()
    endpoint = f"{base_url.rstrip('/')}/rest/v1/active_trades"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": f"return={returning}",
    }

    params = {
        "tags": (
            f'cs.{{"strategy:{strategy}","version:{version}",'
            f'"id:{setup_id}","leg:{leg}","trade:{trade}"}}'
        ),
    }
    payload = {
        "sl_level": sl_level,
    }

    print(
        f"[{log_label}][DB_WRITE] action=patch table=active_trades "
        f"strategy={strategy} version={version} setup_id={setup_id} "
        f"leg={leg} trade={trade} "
        f"payload={json.dumps(payload, default=str, sort_keys=True)}"
    )

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.patch(endpoint, headers=headers, params=params, json=payload)

        response.raise_for_status()

        print(
            f"[{log_label}][DB_APPLIED] action=patch table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id} "
            f"leg={leg} trade={trade}"
        )

        return {
            "success": True,
            "status_code": response.status_code,
            "data": response.json() if response.text else None,
            "error": None,
        }

    except httpx.HTTPStatusError as e:
        response = e.response
        try:
            error_body: Any = response.json() if response is not None and response.text else None
        except Exception:
            error_body = response.text if response is not None else str(e)

        print(
            f"[{log_label}][DB_ERROR] action=patch table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id} "
            f"leg={leg} trade={trade} "
            f"status={response.status_code if response is not None else 'unknown'} "
            f"error={json.dumps(error_body, default=str, sort_keys=True)}"
        )

        return {
            "success": False,
            "status_code": response.status_code if response is not None else None,
            "data": None,
            "error": error_body,
        }

    except Exception as e:
        print(
            f"[{log_label}][DB_ERROR] action=patch table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id} "
            f"leg={leg} trade={trade} "
            f"error={str(e)}"
        )

        return {
            "success": False,
            "status_code": None,
            "data": None,
            "error": str(e),
        }
