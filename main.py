import datetime as dt
from typing import Any, Dict, List, Optional
from indicator_bot import IndicatorBot
from zone_finder import build_symbol_zone_map





import asyncio
from fastapi import FastAPI, HTTPException, Query
from contextlib import asynccontextmanager


from candle_engine import (
    CandleEngine,
    init_engine_from_env,
    SUPPORTED_TFS,
)



@asynccontextmanager
async def lifespan(app: FastAPI):
    engine = await init_engine_from_env()
    app.state.candle_engine = engine

    loop = asyncio.get_event_loop()
    loop.create_task(engine.run_loop(interval_seconds=180))

    indicator_bot = IndicatorBot(engine=engine)
    app.state.indicator_bot = indicator_bot
    loop.create_task(indicator_bot.run_loop(interval_seconds=60))

    # App is ready
    yield

    # Optional: shutdown logic later


app = FastAPI(
    title="Trading Brain",
    version="1.0.0",
    lifespan=lifespan,
)


"""
@app.on_event("startup")
async def on_startup():
    
    engine = await init_engine_from_env()
    app.state.candle_engine = engine

    # Start CandleEngine loop
    loop = asyncio.get_event_loop()
    loop.create_task(engine.run_loop(interval_seconds=180))

    # ----------------------------------------------------
    # Initialize and start IndicatorBot
    # ----------------------------------------------------
    indicator_bot = IndicatorBot(engine=engine)
    app.state.indicator_bot = indicator_bot  # optional, but handy

    loop.create_task(indicator_bot.run_loop(interval_seconds=60))
    """


def get_engine() -> CandleEngine:
    engine = getattr(app.state, "candle_engine", None)
    if engine is None:
        raise HTTPException(status_code=503, detail="CandleEngine not ready yet.")
    return engine


@app.get("/health")
async def health() -> Dict[str, Any]:
    engine = getattr(app.state, "candle_engine", None)
    if engine is None:
        return {
            "status": "initializing",
            "symbols": [],
            "timeframes": SUPPORTED_TFS,
        }

    return {
        "status": "ok",
        "symbols": list(engine.symbols),
        "timeframes": SUPPORTED_TFS,
    }


@app.get("/symbols")
async def list_symbols() -> Dict[str, List[str]]:
    engine = get_engine()
    return {"symbols": list(engine.symbols)}


@app.get("/timeframes")
async def list_timeframes(symbol: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    engine = get_engine()

    if symbol is None:
        return {"timeframes": SUPPORTED_TFS}

    sym = symbol.upper()
    if sym not in engine.symbols:
        raise HTTPException(status_code=404, detail=f"Symbol {sym} is not tracked.")

    tfs = sorted(engine.candles.get(sym, {}).keys())
    return {"symbol": sym, "timeframes": tfs or SUPPORTED_TFS}


@app.get("/candles")
async def get_candles_api(
    symbol: str = Query(..., description="Ticker symbol, e.g. SPY"),
    timeframe: str = Query(..., description="Timeframe, e.g. 5m"),
    limit: int = Query(500, ge=1, le=5000),
    from_ts: Optional[str] = Query(None, description="ISO8601 start time filter"),
    to_ts: Optional[str] = Query(None, description="ISO8601 end time filter"),
) -> Dict[str, Any]:
    engine = get_engine()

    sym = symbol.upper()
    tf = timeframe

    if sym not in engine.symbols:
        raise HTTPException(status_code=404, detail=f"Symbol {sym} is not tracked.")

    if tf not in SUPPORTED_TFS:
        raise HTTPException(status_code=400, detail=f"Timeframe {tf} is not supported.")

    candles = engine.get_candles(sym, tf)

    def parse_ts(ts_str: str) -> dt.datetime:
        return dt.datetime.fromisoformat(ts_str)

    if from_ts:
        start = parse_ts(from_ts)
        candles = [c for c in candles if dt.datetime.fromisoformat(c["ts"]) >= start]

    if to_ts:
        end = parse_ts(to_ts)
        candles = [c for c in candles if dt.datetime.fromisoformat(c["ts"]) <= end]

    candles = sorted(candles, key=lambda c: c["ts"])

    if len(candles) > limit:
        candles = candles[-limit:]

    return {
        "symbol": sym,
        "timeframe": tf,
        "count": len(candles),
        "candles": candles,
    }


@app.get("/latest")
async def get_latest_api(
    symbol: str = Query(...),
    timeframe: str = Query(...),
    limit: int = Query(1, ge=1, le=50),
) -> Dict[str, Any]:
    engine = get_engine()
    sym = symbol.upper()
    tf = timeframe

    if sym not in engine.symbols:
        raise HTTPException(status_code=404, detail=f"Symbol {sym} is not tracked.")

    if tf not in SUPPORTED_TFS:
        raise HTTPException(status_code=400, detail=f"Timeframe {tf} is not supported.")

    candles = engine.get_candles(sym, tf)
    candles = sorted(candles, key=lambda c: c["ts"])

    if len(candles) > limit:
        candles = candles[-limit:]

    return {
        "symbol": sym,
        "timeframe": tf,
        "count": len(candles),
        "candles": candles,
    }


@app.get("/snapshot")
async def snapshot_api(
    symbol: str = Query(...),
    max_per_tf: int = Query(500, ge=1, le=2000),
) -> Dict[str, Any]:
    engine = get_engine()
    sym = symbol.upper()

    if sym not in engine.symbols:
        raise HTTPException(status_code=404, detail=f"Symbol {sym} is not tracked.")

    result: Dict[str, List[Dict[str, Any]]] = {}
    symbol_candles = engine.candles.get(sym, {})
    for tf in SUPPORTED_TFS:
        tf_candles = symbol_candles.get(tf, [])
        if not tf_candles:
            continue
        tf_candles = sorted(tf_candles, key=lambda c: c["ts"])
        if len(tf_candles) > max_per_tf:
            tf_candles = tf_candles[-max_per_tf:]
        result[tf] = tf_candles

    return {
        "symbol": sym,
        "timeframes": result,
    }
