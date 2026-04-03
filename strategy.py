from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, time
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple


ET = ZoneInfo("America/New_York")


@dataclass
class StrategyConfig:
    strategy_name: str = "SPY_VWAP_Pullback_Scalp_Sim"
    symbol: str = "SPY"
    opening_range_minutes: int = 5
    bias_mode: str = "fixed_bias_for_day"  # fixed_bias_for_day | flipping_bias
    vwap_tolerance_percent: float = 0.0005
    vwap_tolerance_points: float = 0.10
    extension_mode: str = "points"  # percent | points
    min_vwap_extension_percent: float = 0.0015
    min_vwap_extension_points: float = 0.30
    confirmation_type: str = "either_confirmation"  # breakout_confirmation | strong_close_confirmation | either_confirmation
    strong_close_threshold_percent: float = 0.70
    entry_fill_method: str = "next_candle_open"  # next_candle_open | confirmation_close
    stop_method: str = "pullback_candle"  # pullback_candle | confirmation_candle | fixed_percent | atr
    fixed_stop_percent: float = 0.0025
    atr_length: int = 14
    atr_multiplier: float = 1.5
    take_profit_mode: str = "partial_1R_2R"  # fixed_R | partial_1R_2R | trail_prev_candle | opposite_signal
    target_R_1: float = 1.0
    target_R_2: float = 2.0
    partial_take_profit_enabled: bool = True
    minimum_candle_body_size: float = 0.03
    midday_skip_enabled: bool = True
    midday_skip_start_time: str = "12:00"
    midday_skip_end_time: str = "13:00"
    max_trades_per_day: int = 6
    minimum_entry_to_stop_distance: float = 0.05
    minimum_entry_to_target_distance: float = 0.05
    minimum_volume: int = 0
    no_new_entries_after_time: str = "15:45"
    forced_flat_time: str = "15:55"
    starting_capital: float = 25_000.0
    position_size_method: str = "percent_risk"  # fixed_shares | fixed_dollar | percent_risk
    fixed_position_size: float = 100
    percent_risk_per_trade: float = 0.005
    commission: float = 0.0
    slippage: float = 0.01
    same_bar_conflict_mode: str = "conservative_stop_first"  # conservative_stop_first | optimistic_target_first | user_selected_priority
    user_selected_priority: str = "stop"
    debug_mode: bool = False
    one_active_trade_at_a_time: bool = True
    pyramiding: bool = False


@dataclass
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class PendingEntry:
    signal_ts: datetime
    side: str
    bias_at_entry: str
    entry_price_hint: float
    pullback_extreme: float
    confirmation_extreme: float
    target_1: float
    target_2: float
    stop_price: float


@dataclass
class Trade:
    trade_number: int
    trade_date: str
    side: str
    bias_at_entry: str
    entry_signal_timestamp: datetime
    entry_fill_timestamp: datetime
    entry_price: float
    stop_price: float
    target_price_1: float
    target_price_2: float
    position_size: int
    risk_per_share_or_unit: float
    total_risk_amount: float
    remaining_size: int
    partial_exit_1_timestamp: Optional[datetime] = None
    partial_exit_1_price: Optional[float] = None
    partial_exit_1_size: int = 0
    partial_exit_1_pnl: float = 0.0
    exit_timestamp: Optional[datetime] = None
    exit_price: Optional[float] = None
    final_exit_timestamp: Optional[datetime] = None
    final_exit_price: Optional[float] = None
    final_exit_size: int = 0
    final_exit_pnl: float = 0.0
    gross_pnl: float = 0.0
    commission_cost: float = 0.0
    slippage_cost: float = 0.0
    net_pnl: float = 0.0
    balance_after_trade: float = 0.0
    total_profit_loss_after_trade: float = 0.0
    R_multiple: float = 0.0
    bars_held: int = 0
    exit_reason: str = ""


class SPYVWAPPullbackScalpSim:
    """Deterministic candle-by-candle simulation strategy.

    Expected input candles: list[dict] with timestamp/open/high/low/close/volume.
    """

    def __init__(self, config: Optional[StrategyConfig] = None):
        self.cfg = config or StrategyConfig()
        self.reset()

    def reset(self) -> None:
        self.current_balance = self.cfg.starting_capital
        self.gross_profit = 0.0
        self.gross_loss = 0.0
        self.cumulative_profit = 0.0
        self.cumulative_loss = 0.0
        self.total_net_profit = 0.0
        self.max_equity_to_date = self.cfg.starting_capital
        self.drawdown_to_date = 0.0
        self.trade_log: List[Dict[str, Any]] = []
        self.equity_curve: List[Dict[str, Any]] = []
        self.daily_summary: Dict[str, Dict[str, Any]] = {}
        self.debug_log: List[Dict[str, Any]] = []

    def _parse_time(self, hhmm: str) -> time:
        h, m = hhmm.split(":")
        return time(int(h), int(m), 0)

    def _to_candle(self, row: Dict[str, Any]) -> Candle:
        ts = row["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=ET)
        ts = ts.astimezone(ET)
        return Candle(ts, float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"]), float(row.get("volume", 0.0)))

    def _in_rth(self, ts: datetime) -> bool:
        return time(9, 30) <= ts.timetz().replace(tzinfo=None) <= time(16, 0)

    def _session_date(self, ts: datetime) -> str:
        return ts.date().isoformat()

    def _update_daily(self, d: str) -> None:
        if d not in self.daily_summary:
            self.daily_summary[d] = {
                "trading_date": d,
                "daily_trade_count": 0,
                "daily_win_count": 0,
                "daily_loss_count": 0,
                "daily_gross_profit": 0.0,
                "daily_gross_loss": 0.0,
                "daily_net_profit": 0.0,
                "daily_start_balance": self.current_balance,
                "daily_end_balance": self.current_balance,
                "long_trade_count": 0,
                "short_trade_count": 0,
            }

    def _atr(self, candles: List[Candle], idx: int) -> Optional[float]:
        if idx < 1:
            return None
        start = max(1, idx - self.cfg.atr_length + 1)
        trs = []
        for i in range(start, idx + 1):
            c = candles[i]
            p = candles[i - 1]
            tr = max(c.high - c.low, abs(c.high - p.close), abs(c.low - p.close))
            trs.append(tr)
        return sum(trs) / len(trs) if trs else None

    def _same_bar_priority(self, side: str) -> str:
        mode = self.cfg.same_bar_conflict_mode
        if mode == "conservative_stop_first":
            return "stop"
        if mode == "optimistic_target_first":
            return "target"
        return self.cfg.user_selected_priority

    def _risk_position_size(self, entry: float, stop: float) -> int:
        risk_per_share = abs(entry - stop)
        if risk_per_share <= 0:
            return 0
        if self.cfg.position_size_method == "fixed_shares":
            return int(self.cfg.fixed_position_size)
        if self.cfg.position_size_method == "fixed_dollar":
            return int(self.cfg.fixed_position_size / entry)
        risk_dollars = self.current_balance * self.cfg.percent_risk_per_trade
        return max(0, int(risk_dollars / risk_per_share))

    def _build_targets(self, side: str, entry: float, stop: float) -> Tuple[float, float, float]:
        risk = abs(entry - stop)
        t1 = entry + (risk * self.cfg.target_R_1 if side == "long" else -risk * self.cfg.target_R_1)
        t2 = entry + (risk * self.cfg.target_R_2 if side == "long" else -risk * self.cfg.target_R_2)
        return risk, t1, t2

    def run(self, raw_candles: List[Dict[str, Any]]) -> Dict[str, Any]:
        self.reset()
        candles = [self._to_candle(c) for c in raw_candles]
        candles.sort(key=lambda x: x.timestamp)

        no_new_after = self._parse_time(self.cfg.no_new_entries_after_time)
        forced_flat = self._parse_time(self.cfg.forced_flat_time)
        mid_s = self._parse_time(self.cfg.midday_skip_start_time)
        mid_e = self._parse_time(self.cfg.midday_skip_end_time)

        day_state: Dict[str, Any] = {}
        pending: Optional[PendingEntry] = None
        trade: Optional[Trade] = None
        trade_num = 0

        for i, c in enumerate(candles):
            if not self._in_rth(c.timestamp):
                continue

            d = self._session_date(c.timestamp)
            self._update_daily(d)
            if day_state.get("date") != d:
                day_state = {
                    "date": d,
                    "or_high": None,
                    "or_low": None,
                    "or_done": False,
                    "bias": None,
                    "bias_change_timestamp": None,
                    "extended": False,
                    "pullback_armed": False,
                    "pullback_extreme": None,
                    "trades": 0,
                    "cum_pv": 0.0,
                    "cum_vol": 0.0,
                }

            # 1-3 Session/opening range/VWAP
            day_state["cum_pv"] += c.close * c.volume
            day_state["cum_vol"] += c.volume
            vwap = day_state["cum_pv"] / day_state["cum_vol"] if day_state["cum_vol"] > 0 else c.close

            or_start = datetime.combine(c.timestamp.date(), time(9, 30), tzinfo=ET)
            or_end_minute = 30 + self.cfg.opening_range_minutes - 1
            or_end = datetime.combine(c.timestamp.date(), time(9, or_end_minute), tzinfo=ET).replace(second=59)
            if or_start <= c.timestamp <= or_end:
                day_state["or_high"] = c.high if day_state["or_high"] is None else max(day_state["or_high"], c.high)
                day_state["or_low"] = c.low if day_state["or_low"] is None else min(day_state["or_low"], c.low)
            if c.timestamp > or_end:
                day_state["or_done"] = True

            # 4 bias
            if day_state["or_done"] and day_state["or_high"] is not None and day_state["or_low"] is not None:
                new_bias = day_state["bias"]
                if c.close > day_state["or_high"]:
                    new_bias = "bullish"
                elif c.close < day_state["or_low"]:
                    new_bias = "bearish"

                if new_bias and new_bias != day_state["bias"]:
                    if day_state["bias"] is None or self.cfg.bias_mode == "flipping_bias":
                        day_state["bias"] = new_bias
                        day_state["bias_change_timestamp"] = c.timestamp
                        day_state["extended"] = False
                        day_state["pullback_armed"] = False

            # fill pending entry at next bar open
            if pending and self.cfg.entry_fill_method == "next_candle_open" and c.timestamp > pending.signal_ts and trade is None:
                fill = c.open + (self.cfg.slippage if pending.side == "long" else -self.cfg.slippage)
                pos = self._risk_position_size(fill, pending.stop_price)
                if pos > 0:
                    trade_num += 1
                    risk_ps, t1, t2 = self._build_targets(pending.side, fill, pending.stop_price)
                    trade = Trade(
                        trade_number=trade_num,
                        trade_date=d,
                        side=pending.side,
                        bias_at_entry=pending.bias_at_entry,
                        entry_signal_timestamp=pending.signal_ts,
                        entry_fill_timestamp=c.timestamp,
                        entry_price=fill,
                        stop_price=pending.stop_price,
                        target_price_1=t1,
                        target_price_2=t2,
                        position_size=pos,
                        remaining_size=pos,
                        risk_per_share_or_unit=risk_ps,
                        total_risk_amount=risk_ps * pos,
                    )
                    day_state["trades"] += 1
                    self.daily_summary[d]["daily_trade_count"] += 1
                    self.daily_summary[d][f"{'long' if pending.side == 'long' else 'short'}_trade_count"] += 1
                pending = None

            # entry on confirmation close method
            # 5-8 extension/pullback/confirmation/entry signal creation
            if day_state["or_done"] and trade is None and pending is None and day_state["bias"]:
                blocked_reason = None
                nowt = c.timestamp.timetz().replace(tzinfo=None)
                body = abs(c.close - c.open)
                if self.cfg.midday_skip_enabled and mid_s <= nowt <= mid_e:
                    blocked_reason = "midday_filter"
                elif nowt >= no_new_after:
                    blocked_reason = "entry_cutoff"
                elif day_state["trades"] >= self.cfg.max_trades_per_day:
                    blocked_reason = "max_trades"
                elif body < self.cfg.minimum_candle_body_size:
                    blocked_reason = "min_body"
                elif self.cfg.minimum_volume > 0 and c.volume < self.cfg.minimum_volume:
                    blocked_reason = "min_volume"

                if not blocked_reason:
                    bias = day_state["bias"]
                    if bias == "bullish":
                        ext_ok = (c.high - vwap) >= (self.cfg.min_vwap_extension_points if self.cfg.extension_mode == "points" else vwap * self.cfg.min_vwap_extension_percent)
                        day_state["extended"] = day_state["extended"] or ext_ok
                        tol = self.cfg.vwap_tolerance_points if self.cfg.vwap_tolerance_points > 0 else vwap * self.cfg.vwap_tolerance_percent
                        if day_state["extended"] and c.low <= (vwap + tol):
                            day_state["pullback_armed"] = True
                            day_state["pullback_extreme"] = c.low

                        prev = candles[i - 1] if i > 0 else None
                        if prev and day_state["pullback_armed"]:
                            breakout = c.close > prev.high and c.close > c.open
                            rng = max(1e-9, c.high - c.low)
                            strong = ((c.close - c.low) / rng) >= self.cfg.strong_close_threshold_percent
                            confirm = (self.cfg.confirmation_type == "breakout_confirmation" and breakout) or (self.cfg.confirmation_type == "strong_close_confirmation" and strong) or (self.cfg.confirmation_type == "either_confirmation" and (breakout or strong))
                            if confirm and c.close > vwap:
                                stop = day_state["pullback_extreme"] if self.cfg.stop_method == "pullback_candle" else c.low
                                if self.cfg.stop_method == "fixed_percent":
                                    stop = c.close * (1 - self.cfg.fixed_stop_percent)
                                elif self.cfg.stop_method == "atr":
                                    atr = self._atr(candles, i) or (c.high - c.low)
                                    stop = c.close - (atr * self.cfg.atr_multiplier)
                                entry_hint = c.close
                                risk, t1, t2 = self._build_targets("long", entry_hint, stop)
                                if (entry_hint - stop) >= self.cfg.minimum_entry_to_stop_distance and (t1 - entry_hint) >= self.cfg.minimum_entry_to_target_distance:
                                    pending = PendingEntry(c.timestamp, "long", bias, entry_hint, day_state["pullback_extreme"], c.low, t1, t2, stop)
                    else:
                        ext_ok = (vwap - c.low) >= (self.cfg.min_vwap_extension_points if self.cfg.extension_mode == "points" else vwap * self.cfg.min_vwap_extension_percent)
                        day_state["extended"] = day_state["extended"] or ext_ok
                        tol = self.cfg.vwap_tolerance_points if self.cfg.vwap_tolerance_points > 0 else vwap * self.cfg.vwap_tolerance_percent
                        if day_state["extended"] and c.high >= (vwap - tol):
                            day_state["pullback_armed"] = True
                            day_state["pullback_extreme"] = c.high

                        prev = candles[i - 1] if i > 0 else None
                        if prev and day_state["pullback_armed"]:
                            breakout = c.close < prev.low and c.close < c.open
                            rng = max(1e-9, c.high - c.low)
                            strong = ((c.high - c.close) / rng) >= self.cfg.strong_close_threshold_percent
                            confirm = (self.cfg.confirmation_type == "breakout_confirmation" and breakout) or (self.cfg.confirmation_type == "strong_close_confirmation" and strong) or (self.cfg.confirmation_type == "either_confirmation" and (breakout or strong))
                            if confirm and c.close < vwap:
                                stop = day_state["pullback_extreme"] if self.cfg.stop_method == "pullback_candle" else c.high
                                if self.cfg.stop_method == "fixed_percent":
                                    stop = c.close * (1 + self.cfg.fixed_stop_percent)
                                elif self.cfg.stop_method == "atr":
                                    atr = self._atr(candles, i) or (c.high - c.low)
                                    stop = c.close + (atr * self.cfg.atr_multiplier)
                                entry_hint = c.close
                                risk, t1, t2 = self._build_targets("short", entry_hint, stop)
                                if (stop - entry_hint) >= self.cfg.minimum_entry_to_stop_distance and (entry_hint - t1) >= self.cfg.minimum_entry_to_target_distance:
                                    pending = PendingEntry(c.timestamp, "short", bias, entry_hint, day_state["pullback_extreme"], c.high, t1, t2, stop)

                if self.cfg.debug_mode:
                    self.debug_log.append({
                        "timestamp": c.timestamp.isoformat(),
                        "opening_range_high": day_state["or_high"],
                        "opening_range_low": day_state["or_low"],
                        "current_bias": day_state["bias"],
                        "bias_change_timestamp": day_state["bias_change_timestamp"].isoformat() if day_state["bias_change_timestamp"] else None,
                        "extension_away_from_vwap": day_state["extended"],
                        "pullback_armed": day_state["pullback_armed"],
                        "entry_block_reason": blocked_reason,
                    })

            if pending and self.cfg.entry_fill_method == "confirmation_close" and trade is None:
                fill = pending.entry_price_hint + (self.cfg.slippage if pending.side == "long" else -self.cfg.slippage)
                pos = self._risk_position_size(fill, pending.stop_price)
                if pos > 0:
                    trade_num += 1
                    risk_ps, t1, t2 = self._build_targets(pending.side, fill, pending.stop_price)
                    trade = Trade(
                        trade_number=trade_num,
                        trade_date=d,
                        side=pending.side,
                        bias_at_entry=pending.bias_at_entry,
                        entry_signal_timestamp=pending.signal_ts,
                        entry_fill_timestamp=pending.signal_ts,
                        entry_price=fill,
                        stop_price=pending.stop_price,
                        target_price_1=t1,
                        target_price_2=t2,
                        position_size=pos,
                        remaining_size=pos,
                        risk_per_share_or_unit=risk_ps,
                        total_risk_amount=risk_ps * pos,
                    )
                    day_state["trades"] += 1
                pending = None

            # 10 exits for active trade
            if trade:
                trade.bars_held += 1
                nowt = c.timestamp.timetz().replace(tzinfo=None)
                side = trade.side
                stop_hit = c.low <= trade.stop_price if side == "long" else c.high >= trade.stop_price
                t1_hit = c.high >= trade.target_price_1 if side == "long" else c.low <= trade.target_price_1
                t2_hit = c.high >= trade.target_price_2 if side == "long" else c.low <= trade.target_price_2
                same_bar_conflict = stop_hit and (t1_hit or t2_hit)
                if same_bar_conflict:
                    first = self._same_bar_priority(side)
                    if first == "stop":
                        t1_hit = t2_hit = False
                    else:
                        stop_hit = False

                # partial at target 1
                if self.cfg.partial_take_profit_enabled and t1_hit and trade.partial_exit_1_timestamp is None:
                    px = trade.target_price_1
                    size = trade.remaining_size // 2
                    pnl = (px - trade.entry_price) * size if side == "long" else (trade.entry_price - px) * size
                    trade.partial_exit_1_timestamp = c.timestamp
                    trade.partial_exit_1_price = px
                    trade.partial_exit_1_size = size
                    trade.partial_exit_1_pnl = pnl
                    trade.remaining_size -= size

                exit_reason = None
                exit_price = None
                if t2_hit:
                    exit_reason = "target_2"
                    exit_price = trade.target_price_2
                elif stop_hit:
                    exit_reason = "stop"
                    exit_price = trade.stop_price
                elif self.cfg.take_profit_mode == "trail_prev_candle" and i > 0:
                    p = candles[i - 1]
                    trail = p.low if side == "long" else p.high
                    if (side == "long" and c.low <= trail) or (side == "short" and c.high >= trail):
                        exit_reason = "trail"
                        exit_price = trail
                elif nowt >= forced_flat:
                    exit_reason = "forced_eod"
                    exit_price = c.close

                if exit_reason:
                    size = trade.remaining_size
                    pnl = (exit_price - trade.entry_price) * size if side == "long" else (trade.entry_price - exit_price) * size
                    trade.final_exit_timestamp = c.timestamp
                    trade.final_exit_price = exit_price
                    trade.final_exit_size = size
                    trade.final_exit_pnl = pnl
                    trade.exit_timestamp = c.timestamp
                    trade.exit_price = exit_price
                    trade.exit_reason = exit_reason
                    trade.gross_pnl = trade.partial_exit_1_pnl + trade.final_exit_pnl
                    trade.commission_cost = self.cfg.commission
                    trade.slippage_cost = self.cfg.slippage * trade.position_size
                    trade.net_pnl = trade.gross_pnl - trade.commission_cost - trade.slippage_cost
                    trade.R_multiple = trade.net_pnl / trade.total_risk_amount if trade.total_risk_amount > 0 else 0.0

                    self.current_balance += trade.net_pnl
                    self.total_net_profit += trade.net_pnl
                    if trade.net_pnl >= 0:
                        self.gross_profit += trade.net_pnl
                        self.cumulative_profit += trade.net_pnl
                        self.daily_summary[d]["daily_win_count"] += 1
                    else:
                        self.gross_loss += abs(trade.net_pnl)
                        self.cumulative_loss += abs(trade.net_pnl)
                        self.daily_summary[d]["daily_loss_count"] += 1

                    trade.balance_after_trade = self.current_balance
                    trade.total_profit_loss_after_trade = self.total_net_profit
                    self.max_equity_to_date = max(self.max_equity_to_date, self.current_balance)
                    self.drawdown_to_date = max(self.drawdown_to_date, self.max_equity_to_date - self.current_balance)

                    self.daily_summary[d]["daily_gross_profit"] += max(0.0, trade.gross_pnl)
                    self.daily_summary[d]["daily_gross_loss"] += min(0.0, trade.gross_pnl)
                    self.daily_summary[d]["daily_net_profit"] += trade.net_pnl
                    self.daily_summary[d]["daily_end_balance"] = self.current_balance

                    self.trade_log.append(asdict(trade))
                    self.equity_curve.append({
                        "trade_number": trade.trade_number,
                        "trade_date": trade.trade_date,
                        "equity_value": self.current_balance,
                    })
                    trade = None

        return self._report()

    def _report(self) -> Dict[str, Any]:
        wins = [t for t in self.trade_log if t["net_pnl"] > 0]
        losses = [t for t in self.trade_log if t["net_pnl"] < 0]
        breakeven = [t for t in self.trade_log if t["net_pnl"] == 0]
        total = len(self.trade_log)
        avg_win = sum(t["net_pnl"] for t in wins) / len(wins) if wins else 0.0
        avg_loss = sum(t["net_pnl"] for t in losses) / len(losses) if losses else 0.0
        total_r = sum(t["R_multiple"] for t in self.trade_log)

        entries_bucket: Dict[str, int] = {}
        for t in self.trade_log:
            ts = t["entry_fill_timestamp"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            bucket = f"{ts.hour:02d}:00"
            entries_bucket[bucket] = entries_bucket.get(bucket, 0) + 1

        return {
            "strategy_name": self.cfg.strategy_name,
            "inputs": asdict(self.cfg),
            "entry_rules": {
                "requires_bias": True,
                "requires_vwap_side": True,
                "requires_extension_then_pullback": True,
                "requires_confirmation_close": True,
                "default_execution": "next candle open",
                "one_active_trade_at_a_time": True,
            },
            "exit_rules": {
                "stop_method": self.cfg.stop_method,
                "take_profit_mode": self.cfg.take_profit_mode,
                "partial_take_profit_enabled": self.cfg.partial_take_profit_enabled,
                "forced_eod": self.cfg.forced_flat_time,
            },
            "assumptions": {
                "completed_candle_only": True,
                "lookahead": False,
                "fill": "full fills",
                "market_impact": "not modeled",
                "partial_fill": "disabled",
                "same_bar_conflict_mode": self.cfg.same_bar_conflict_mode,
            },
            "same_bar_conflict_rule_used": self.cfg.same_bar_conflict_mode,
            "trade_log_format": list(self.trade_log[0].keys()) if self.trade_log else [],
            "equity_curve_format": ["trade_number", "trade_date", "equity_value"],
            "performance": {
                "total_trades": total,
                "winning_trades": len(wins),
                "losing_trades": len(losses),
                "breakeven_trades": len(breakeven),
                "win_rate": (len(wins) / total) if total else 0.0,
                "gross_profit": self.gross_profit,
                "gross_loss": self.gross_loss,
                "net_profit": self.total_net_profit,
                "final_account_balance": self.current_balance,
                "average_win": avg_win,
                "average_loss": avg_loss,
                "largest_win": max((t["net_pnl"] for t in wins), default=0.0),
                "largest_loss": min((t["net_pnl"] for t in losses), default=0.0),
                "average_R_multiple": (total_r / total) if total else 0.0,
                "total_R": total_r,
                "profit_factor": (self.gross_profit / self.gross_loss) if self.gross_loss else 0.0,
                "expectancy": (self.total_net_profit / total) if total else 0.0,
                "max_drawdown": self.drawdown_to_date,
                "max_drawdown_percent": (self.drawdown_to_date / self.max_equity_to_date) if self.max_equity_to_date else 0.0,
                "average_bars_held": (sum(t["bars_held"] for t in self.trade_log) / total) if total else 0.0,
                "average_trade_duration_if_available": None,
                "long_trade_count": sum(1 for t in self.trade_log if t["side"] == "long"),
                "short_trade_count": sum(1 for t in self.trade_log if t["side"] == "short"),
                "pnl_by_day": {d: s["daily_net_profit"] for d, s in self.daily_summary.items()},
                "entries_by_time_bucket": entries_bucket,
            },
            "trade_log": self.trade_log,
            "equity_curve": self.equity_curve,
            "daily_summary": list(self.daily_summary.values()),
            "debug_log": self.debug_log,
            "spy_testing_notes": "Run on SPY 1m and 5m RTH candles with timezone-aware timestamps in America/New_York."
        }


def run_strategy(raw_candles: List[Dict[str, Any]], config_overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = StrategyConfig(**(config_overrides or {}))
    return SPYVWAPPullbackScalpSim(cfg).run(raw_candles)
