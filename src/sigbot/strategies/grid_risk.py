"""Frozen single-market plans and projections from the existing common ledger.

Risk is a declared stress scenario, never a guaranteed loss ceiling. No fills,
books, fees, balances or execution probabilities are manufactured here.
"""

import json
from collections import defaultdict, deque
from datetime import timedelta
from decimal import ROUND_FLOOR
from decimal import Decimal as D
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import Field
from sqlalchemy import func, select

from sigbot.domain import ZERO, Money, content_hash, utc_time
from sigbot.errors import RiskRejected
from sigbot.storage import (
    FillRow,
    Instrument,
    IntentRow,
    Journal,
    Order,
    PositionLot,
    StrategyEvent,
)
from sigbot.strategies.grid_sig import Parameters as Legacy
from sigbot.strategies.grid_sig import ceil_tick, floor_tick, profile

IDENTITY = ("grid_sig", "2.0")


class Parameters(Legacy):
    window_hours: int = Field(default=6, ge=6, le=48, strict=True)
    strategy_cash: Money = Field(default=D(50000), gt=0, le=50000)
    order_cash: Money = Field(
        default=D(500), gt=0, le=500
    )  # Native notional guard, not sizing rule.
    market_cash: Money = Field(default=D(500), gt=0, le=500)
    market_risk: Money = Field(default=D(10), gt=0, le=10)
    total_risk: Money = Field(default=D(500), gt=0, le=500)
    daily_loss: Money = Field(default=D(250), gt=0, le=250)
    market_daily_loss: Money = Field(default=D(20), gt=0, le=20)
    stop_widths: int = Field(default=1, ge=1, le=3, strict=True)
    target_kind: str = Field(default="ALPHA_0.75", pattern=r"^(ALPHA_0\.(5|75)|ALPHA_1|Q70|Q80)$")
    holding_seconds: int = Field(default=3600, ge=1800, le=10800, strict=True)
    stress_ticks: int = Field(default=2, ge=1, le=2, strict=True)


def quantile(values, fraction):
    values = sorted(values)
    x = D(len(values) - 1) * fraction
    i = int(x)
    return values[i] + (values[min(i + 1, len(values) - 1)] - values[i]) * (x - i)


def directional_profile(rows, at, p, outcome):
    # Transform every NO sample before computing quantiles (1-Q30, not 1-Q70).
    if outcome == "NO":
        rows = [{**r, "mean": str(1 - D(r["mean"]))} for r in rows]
    f = profile(rows, at, p)
    if f.get("mu"):
        left, right = utc_time(f["asof"]) - timedelta(hours=p.window_hours), utc_time(f["asof"])
        values = [
            D(r["mean"])
            for r in rows
            if left <= utc_time(r["end"]) < right and utc_time(r["available_at"]) <= at
        ]
        f.update(q70=str(quantile(values, D(".7"))), q80=str(quantile(values, D(".8"))))
    return f


def decimal_text(value):
    return format(value.normalize(), "f")


def frozen_plan(f, tick, p):
    mu, width = D(f["mu"]), D(f["width"])
    first = floor_tick(mu / (1 + width / mu), tick)
    stop = floor_tick(first - p.stop_widths * width, tick)
    if stop < tick:
        raise RiskRejected("GRID_V2_STOP_UNREACHABLE")
    levels = sorted(
        {
            floor_tick(mu / (1 + width / mu) ** j, tick)
            for j in range(1, 4)
            if floor_tick(mu / (1 + width / mu) ** j, tick) >= stop + tick
        },
        reverse=True,
    )
    if p.target_kind.startswith("ALPHA_"):
        target = first + D(p.target_kind.removeprefix("ALPHA_")) * (mu - first)
    else:
        target = D(f[p.target_kind.lower()])
    target = ceil_tick(target, tick)
    if not levels or target < levels[0] + tick or target >= 1:
        raise RiskRejected("GRID_V2_NO_VALID_TARGET")
    return {
        "mu": decimal_text(mu),
        "width": decimal_text(width),
        "levels": [decimal_text(x) for x in levels],
        "stop": decimal_text(stop),
        "target": decimal_text(target),
        "holding_seconds": p.holding_seconds,
        "epsilon": decimal_text(p.stress_ticks * tick),
        "stress_source": "OPERATOR_TICK_STRESS_NOT_EMPIRICAL",
        "parameters": p.model_dump(mode="json"),
        "profile_asof": f["asof"],
    }


def plan_key(plan):
    return content_hash(plan)


def unit_risk(price, plan):
    return max(ZERO, price - D(plan["stop"])) + D(plan["epsilon"])


def family_projection(session, account):
    """Include each lot and still-reserved buy exactly once, including UNKNOWN.

    Legacy/unprojectable risk consumes full cash. Native reservation release is
    the sole source of pending-quantity release; request/cancel states do not
    substitute for an authoritative terminal receipt.
    """
    markets: dict[str, Any] = defaultdict(
        lambda: {"cash": ZERO, "risk": ZERO, "slots": defaultdict(lambda: ZERO)}
    )
    orders = {
        o.id: (o, r)
        for o, r in session.execute(
            select(Order, IntentRow)
            .join(IntentRow, Order.intent_id == IntentRow.id)
            .where(
                Order.account_id == account,
                IntentRow.payload["strategy_id"].as_string() == IDENTITY[0],
            )
        )
    }
    fills = {f.id: f for f in session.scalars(select(FillRow).where(FillRow.account_id == account))}

    def add(inst_id, q, cash, origin):
        inst = session.get(Instrument, inst_id)
        n = json.loads(origin.payload.get("note") or "{}")
        m = markets[inst.exchange_id]
        risk = (
            q * unit_risk(cash / q, n["plan"])
            if q and origin.payload.get("strategy_version") == "2.0"
            else cash
        )
        m["cash"] += cash
        m["risk"] += risk
        if origin.payload.get("strategy_version") == "2.0":
            m["slots"][(n["cycle_id"], n["slot"])] += risk

    for lot in session.scalars(
        select(PositionLot).where(PositionLot.account_id == account, PositionLot.quantity > ZERO)
    ):
        fill = fills.get(lot.economic_id.removeprefix("fill:"))
        pair = orders.get(fill.order_id) if fill else None
        if pair:
            if lot.cost is None:
                raise RiskRejected("GRID_V2_UNKNOWN_LOT_COST")
            add(lot.instrument_id, lot.quantity, lot.cost, pair[1])
    for o, r in orders.values():
        if o.side == "BUY" and o.reserve_cash > ZERO:
            # Full unresolved remainder includes partial fills; filled lots are above.
            add(o.instrument_id, o.reserve_cash / o.price, o.reserve_cash, r)
    sales = realized_sales(session, account)
    for e in session.scalars(
        select(StrategyEvent).where(
            StrategyEvent.account_id == account,
            StrategyEvent.strategy_id == IDENTITY[0],
            StrategyEvent.state != "CLOSED",
        )
    ):
        cycle = e.data.get("cycle_id")
        if cycle:
            loss = max(ZERO, -sum((x["pnl"] for x in sales if x["cycle_id"] == cycle), ZERO))
            markets[e.event_key]["risk"] += loss
    return markets


def realized_sales(session, account):
    """Replay actual fills in native journal insertion order, the FIFO lot order.

    Not a parallel ledger: read-only projection of existing immutable economic
    events. Order/time ties cannot select a convenient cheap lot. Fill timestamps
    determine Beijing reporting dates, not the causal FIFO consumption order.
    """
    journal_order = dict(
        session.execute(
            select(Journal.economic_id, func.min(Journal.id))
            .where(Journal.account_id == account)
            .group_by(Journal.economic_id)
        ).all()
    )
    rows = list(
        session.execute(
            select(FillRow, Order, IntentRow)
            .join(Order, FillRow.order_id == Order.id)
            .join(IntentRow, Order.intent_id == IntentRow.id)
            .where(
                FillRow.account_id == account,
                IntentRow.payload["strategy_id"].as_string() == IDENTITY[0],
            )
        )
    )
    if any("fill:" + r[0].id not in journal_order for r in rows):
        raise RiskRejected("GRID_V2_FIFO_JOURNAL_INCOMPLETE")
    rows.sort(key=lambda r: journal_order["fill:" + r[0].id])
    lots: dict[str, Any] = defaultdict(deque)
    sales: list[dict[str, Any]] = []
    for fill, order, origin in rows:
        v, n = fill.payload, json.loads(origin.payload.get("note") or "{}")
        q, price, fee = D(v["quantity"]), D(v["price"]), D(v["fee"])
        if order.side == "BUY":
            lots[order.instrument_id].append([q, q * price + fee])
            continue
        remaining, cost = q, ZERO
        while remaining:
            if not lots[order.instrument_id]:
                raise RiskRejected("GRID_V2_FIFO_HISTORY_INCOMPLETE")
            lot = lots[order.instrument_id][0]
            take = min(remaining, lot[0])
            consumed = lot[1] * take / lot[0]
            cost += consumed
            lot[0] -= take
            lot[1] -= consumed
            remaining -= take
            if not lot[0]:
                lots[order.instrument_id].popleft()
        stamp = utc_time(v["filled_at"])
        sales.append(
            {
                "fill_id": fill.id,
                "pnl": q * price - fee - cost,
                "at": stamp,
                "exchange_id": session.get(Instrument, order.instrument_id).exchange_id,
                "cycle_id": n.get("cycle_id"),
                "day": stamp.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat(),
            }
        )
    return sales


def daily_gate(session, account, inst, at, p):
    """Persist a day latch in the common strategy event, independent of cycles."""
    day = at.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
    key = "DAY:" + day
    row = session.get(StrategyEvent, (account, *IDENTITY, key))
    if row is None:
        row = StrategyEvent(
            account_id=account,
            strategy_id=IDENTITY[0],
            strategy_version=IDENTITY[1],
            event_key=key,
            instrument_id=inst.id,
            state="CLOSED",
            state_group="RISK_DAY",
            shock_group="SINGLE_MARKET_RISK",
            direction_bucket="ALL",
            future_fee_reserve=ZERO,
            data={},
        )
        session.add(row)
        session.flush()
    nets: dict[str, D]
    net, nets, stopped, market_stops = (
        ZERO,
        defaultdict(lambda: ZERO),
        row.data.get("stopped", False),
        set(row.data.get("markets", [])),
    )
    for x in realized_sales(session, account):
        if x["day"] != day:
            continue
        net += x["pnl"]
        nets[x["exchange_id"]] += x["pnl"]
        stopped |= net <= -p.daily_loss
        if nets[x["exchange_id"]] <= -p.market_daily_loss:
            market_stops.add(x["exchange_id"])
    row.data = {
        "day": day,
        "stopped": stopped,
        "markets": sorted(market_stops),
        "realized": str(net),
        "market_realized": {k: str(v) for k, v in nets.items()},
    }
    return not stopped and inst.exchange_id not in market_stops


def size_order(plan, slot, price, projection, exchange, available, capacity, p):
    m = projection.get(exchange, {"risk": ZERO, "cash": ZERO, "slots": {}})
    risk = unit_risk(price, plan)
    slot_used = m["slots"].get((plan_key(plan), slot), ZERO)
    bounds = [
        (p.market_risk / len(plan["levels"]) - slot_used) / risk,
        (p.market_risk - m["risk"]) / risk,
        (p.total_risk - sum((x["risk"] for x in projection.values()), ZERO)) / risk,
        (p.market_cash - m["cash"]) / price,
        available / price,
        capacity,
    ]
    return max(ZERO, min(bounds).to_integral_value(rounding=ROUND_FLOOR))
