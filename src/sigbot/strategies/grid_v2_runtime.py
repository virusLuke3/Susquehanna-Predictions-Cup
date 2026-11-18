"""V2 producer, using the existing sampler, books, command queue and common OMS."""

import json
from collections import Counter
from datetime import timedelta
from decimal import ROUND_FLOOR
from decimal import Decimal as D
from typing import Any

from sqlalchemy import select

from sigbot.domain import ZERO, content_hash, utc_time
from sigbot.errors import RiskRejected, SigbotError
from sigbot.storage import ExecutionCommand, Instrument, IntentRow, Order, Position, StrategyEvent
from sigbot.strategies.ecr_sig_v1.observer import atomic_json
from sigbot.strategies.grid_execution import (
    VERSION as EXECUTION_VERSION,
)
from sigbot.strategies.grid_execution import (
    buy_review,
    entry_choice,
    executable_exit,
    order_seconds,
    replacement_state,
)
from sigbot.strategies.grid_risk import (
    IDENTITY,
    Parameters,
    daily_gate,
    directional_profile,
    family_projection,
    frozen_plan,
    plan_key,
    size_order,
)
from sigbot.strategies.grid_runtime import GridRunner, now
from sigbot.strategies.grid_sig import finances, own_orders, owned_lots


def quote(inst, at, ttl):
    if (
        not inst.book.get("validated_at")
        or not 0 <= (at - utc_time(inst.book["validated_at"])).total_seconds() <= ttl
        or not inst.book.get("own_orders_known")
    ):
        return None
    bids = sorted(
        [
            (D(x["price"]), D(x["quantity"]) - D(x.get("own_quantity", "0")))
            for x in inst.book.get("bids", [])
        ],
        reverse=True,
    )
    asks = sorted(
        [
            (D(x["price"]), D(x["quantity"]) - D(x.get("own_quantity", "0")))
            for x in inst.book.get("asks", [])
        ]
    )
    bids, asks = [(p, q) for p, q in bids if q > 0], [(p, q) for p, q in asks if q > 0]
    if inst.quality != "HEALTHY" or not bids or not asks or bids[0][0] >= asks[0][0]:
        return None
    return bids, asks


def capacity(inst, plan, at, ttl, active_limit=None):
    book = quote(inst, at, ttl)
    if book is None:
        return ZERO
    bids, asks = book
    # Current external size on BOTH sides bounds passive size and stress exit.
    # Future depth is unknown; scenario risk still includes explicit epsilon.
    exit_depth = sum(
        (q for price, q in bids if price >= D(plan["stop"]) - D(plan["epsilon"])), ZERO
    )
    entry_depth = (
        sum((q for price, q in asks if price <= active_limit), ZERO)
        if active_limit is not None
        else asks[0][1]
    )
    return min(exit_depth, entry_depth)


def e1_diagnostics(evidence, at, tick, plan):
    local = [x for x in evidence if 0 <= (at - utc_time(x["at"])).total_seconds() <= 3600]
    mids = [D(x["mid"]) for x in local]
    span = (utc_time(local[-1]["at"]) - utc_time(local[0]["at"])).total_seconds() if local else 0
    spread = max((D(x["spread"]) for x in local), default=ZERO)
    movement = max(mids) - min(mids) if mids else ZERO
    checks = {
        "observations": len(local) >= 3,
        "span": span >= 600,
        "distinct_midpoints": len(set(mids)) >= 3,
        "movement_exceeds_bounce": movement >= max(2 * tick, spread),
        "past_target_bid_observed": any(D(x["bid"]) >= D(plan["target"]) for x in local),
    }
    return {
        "eligible": all(checks.values()),
        "checks": checks,
        "count": len(local),
        "span_seconds": span,
        "distinct_midpoints": len(set(mids)),
        "mid_range": str(movement),
        "maximum_spread": str(spread),
        "lookback_seconds": 3600,
    }


def e1_gate(evidence, at, tick, plan):
    # A fixed midpoint with alternating trade prints still fails. These are
    # observations and past exit-region coverage, never a profitable-fill claim.
    return e1_diagnostics(evidence, at, tick, plan)["eligible"]


def update_observation(event, inst, at, ttl):
    book = quote(inst, at, ttl)
    if book is None:
        return
    bid, ask = book[0][0][0], book[1][0][0]
    stamp = inst.book["validated_at"]
    e1 = dict(event.data.get("e1", {}))
    values = [
        x for x in e1.get(inst.outcome, []) if 0 <= (at - utc_time(x["at"])).total_seconds() <= 3600
    ]
    if not values or utc_time(stamp) > utc_time(values[-1]["at"]):
        values.append(
            {"at": stamp, "mid": str((bid + ask) / 2), "spread": str(ask - bid), "bid": str(bid)}
        )
    e1[inst.outcome] = values[-720:]
    rearm = dict(event.data.get("rearm", {}))
    if event.state == "CLOSED" and (r := rearm.get(inst.outcome)):
        r = dict(r)
        r["recovered"] = r.get("recovered", False) or bid >= D(r["reference"])
        mid = (bid + ask) / 2
        r["ready"] = (
            r["recovered"] and r.get("last") is not None and mid <= D(r["last"]) - inst.tick
        )
        r["last"] = str(mid)
        rearm[inst.outcome] = r
    event.data = {**event.data, "e1": e1, "rearm": rearm}
    opportunity = event.data.get("opportunity", {})
    if opportunity.get("outcome") == inst.outcome and bid >= D(opportunity["reference"]):
        event.data = {
            **event.data,
            "opportunity": {**opportunity, "ended": True, "ended_at": stamp},
        }


def risk_reason(event, f, bid, at, tick):
    data, plan = dict(event.data), event.data["plan"]
    if data.get("risk_exit"):
        return data["risk_exit"]
    if (
        event.holding_started_at
        and (at - utc_time(event.holding_started_at)).total_seconds() >= plan["holding_seconds"]
    ):
        return "TIMEOUT"
    if bid is not None and bid <= D(plan["stop"]):
        return "STOP"
    if not f.get("eligible"):
        return None  # Missing estimation input never changes a frozen exit.
    threshold = D(plan["mu"]) - max(2 * D(plan["width"]), 2 * tick)
    alarm = data.get("break_watch")
    if D(f["mu"]) < threshold:
        if alarm is None:
            alarm = {"since": at.isoformat(), "buckets": [f["latest_end"]]}
        elif f["latest_end"] not in alarm["buckets"]:
            alarm = {**alarm, "buckets": [*alarm["buckets"], f["latest_end"]]}
        event.data = {**data, "break_watch": alarm}
        if (at - utc_time(alarm["since"])).total_seconds() >= 900 and len(alarm["buckets"]) >= 3:
            event.data = {**event.data, "detected_at": at.isoformat()}
            return "MODEL_BREAK"
    elif alarm:
        event.data = {**data, "break_watch": None}
    return None


class RiskGridRunner(GridRunner):
    identity = IDENTITY
    parameter_class = Parameters

    def __init__(self, runtime, settings, env_file):
        super().__init__(runtime, settings, env_file)
        self.sampler = runtime.grid.sampler  # ONE existing official history worker.
        self.sampler.parameters = self.parameters
        self.history_documents: dict[str, tuple[int, dict[str, Any], str]] = {}

    def make_intent(self, session, inst, quantity, price, note):
        if note["kind"] == "ENTRY":
            note = {**note, "quantity_cap": str(quantity)}
        return super().make_intent(session, inst, quantity, price, note)

    def start(self):
        pass

    def history(self, exchange, timestamp, outcome="YES"):
        path = self.sampler.root / "history" / f"{exchange}.json"
        if not path.exists():
            return {"eligible": False, "reason": "HISTORY_PENDING"}
        version = path.stat().st_mtime_ns
        cached = self.history_documents.get(exchange)
        if cached is None or cached[0] != version:
            document = json.loads(path.read_text())
            cached = version, document, content_hash(document)
            self.history_documents[exchange] = cached
        _, document, digest = cached
        if document["exchange_id"] != exchange or not document.get("window_complete"):
            return {"eligible": False, "reason": "HISTORY_WINDOW_INCOMPLETE"}
        return {
            **directional_profile(document["rows"], timestamp, self.parameters, outcome),
            "history_received_at": document.get("received_at"),
            "history_response_hash": digest,
        }

    def descriptive_windows(self, exchange, timestamp):
        cached = self.history_documents.get(exchange)
        if cached is None:
            return {"exchange_id": exchange, "windows": [], "status": "HISTORY_PENDING"}
        _, document, digest = cached
        if document.get("exchange_id") != exchange or not document.get("window_complete"):
            return {"exchange_id": exchange, "windows": [], "status": "HISTORY_WINDOW_INCOMPLETE"}
        left = document.get("requested_from")
        right = document.get("requested_to")
        windows = []
        for hours in (6, 12, 24, 48):
            p = self.parameters.model_copy(update={"window_hours": hours})
            f = directional_profile(document["rows"], timestamp, p, "YES")
            complete = (
                left is not None
                and right is not None
                and utc_time(left) <= utc_time(f["asof"]) - timedelta(hours=hours)
                and utc_time(right) >= utc_time(f["asof"])
            )
            windows.append(
                {
                    "window_hours": hours,
                    **f,
                    "requested_window_covered": complete,
                    "trading_policy": "DESCRIPTIVE_ONLY_NO_AUTOMATIC_ADMISSION",
                }
            )
        return {
            "exchange_id": exchange,
            "history_received_at": document.get("received_at"),
            "history_response_hash": digest,
            "requested_from": left,
            "requested_to": right,
            "windows": windows,
        }

    def probe_observation(self, at):
        """Break the entry/quote dependency using ONE existing official refresh.

        The shared realtime source may be disconnected. Use the same guarded
        REST adapter, own-order normalization, scheduler and common instrument
        table as execution. No extra collector or unverified endpoint.
        """
        account = self.config.expected_account_id
        with self.store.transaction() as session:
            candidates = []
            for event in session.scalars(
                select(StrategyEvent).where(
                    StrategyEvent.account_id == account,
                    StrategyEvent.strategy_id == IDENTITY[0],
                    StrategyEvent.strategy_version == IDENTITY[1],
                    StrategyEvent.state == "CLOSED",
                    StrategyEvent.state_group == "SINGLE_MARKET",
                )
            ):
                f = self.history(event.event_key, at)
                if not f.get("eligible"):
                    continue
                # Refresh both mapped outcomes through the SAME adapter/budget.
                # Its refresh method updates only the requested instrument.
                side = "YES"
                inst = session.scalar(
                    select(Instrument).where(
                        Instrument.tournament == self.config.expected_tournament_id,
                        Instrument.exchange_id == event.event_key,
                        Instrument.outcome == side,
                    )
                )
                if inst:
                    ids = list(
                        session.scalars(
                            select(Instrument.id).where(
                                Instrument.tournament == self.config.expected_tournament_id,
                                Instrument.exchange_id == event.event_key,
                            )
                        )
                    )
                    candidates.append(
                        (event.data.get("e1_probe_at", ""), inst.id, ids, event.event_key)
                    )
            chosen = min(candidates, default=None)
        if chosen is None:
            return
        _, yes_key, keys, exchange = chosen
        error = None
        try:
            with self.runtime.activate(IDENTITY, "READ"):
                # refresh_book atomically persists both outcomes from one
                # official YES-normalized snapshot and one own-order list.
                self.runtime.refresh(yes_key)
        except SigbotError as exc:
            error = exc.code
        with self.store.transaction() as session:
            self.store.lock_account(session, account)
            event = session.get(StrategyEvent, (account, *IDENTITY, exchange))
            event.data = {**event.data, "e1_probe_at": now().isoformat(), "e1_probe_error": error}
            if error is None:
                for key in keys:
                    update_observation(
                        event, session.get(Instrument, key), now(), self.config.book_ttl_seconds
                    )

    def cycle(self):
        at = now()
        if at < self.next_scan:
            return
        self.next_scan = at + timedelta(seconds=10)
        p, account = self.parameters, self.config.expected_account_id
        with self.store.transaction() as session:
            tracked = [
                (e, session.get(Instrument, e.instrument_id))
                for e in session.scalars(
                    select(StrategyEvent).where(
                        StrategyEvent.account_id == account,
                        StrategyEvent.strategy_id == IDENTITY[0],
                        StrategyEvent.strategy_version == IDENTITY[1],
                        StrategyEvent.state != "CLOSED",
                    )
                )
            ]
            oldest_pair = min(
                tracked,
                key=lambda pair: (
                    pair[0].data.get("entry_replacement", {}).get("phase") != "READY",
                    utc_time(pair[1].book.get("validated_at") or pair[1].book_at),
                ),
                default=None,
            )
            oldest = oldest_pair[1] if oldest_pair else None
        self.probe_observation(at)
        at = now()
        if oldest:
            with self.runtime.activate(IDENTITY, "READ"):
                self.runtime.refresh(oldest.id)
            at = now()

        candidates: list[tuple[D | int, Any]] = []
        cancels: dict[str, str] = {}
        reasons: Counter[str] = Counter()
        profiles: list[dict] = []
        diagnostics: list[dict] = []
        shadow_profiles: list[dict] = []
        with self.store.transaction() as session:
            self.store.lock_account(session, account)
            all_inst = list(
                session.scalars(
                    select(Instrument).where(
                        Instrument.tournament == self.config.expected_tournament_id
                    )
                )
            )
            markets: dict[str, dict[str, Any]] = {}
            for inst in all_inst:
                markets.setdefault(inst.exchange_id, {})[inst.outcome] = inst
            active = own_orders(session, account, active=True, identity=IDENTITY)
            own = owned_lots(session, account, identity=IDENTITY)
            owned_ids = {lot.instrument_id for lot, _ in own}
            by_id = {i.id: i for i in all_inst}
            protected = set()
            for pos in session.scalars(
                select(Position).where(Position.account_id == account, Position.quantity > ZERO)
            ):
                if pos.instrument_id not in owned_ids:
                    protected.add(by_id[pos.instrument_id].exchange_id)
            for o, r in session.execute(
                select(Order, IntentRow)
                .join(IntentRow, Order.intent_id == IntentRow.id)
                .where(Order.account_id == account)
            ):
                if (
                    o.reserve_cash
                    or o.reserve_quantity
                    or o.request_state in {"SENDING", "UNKNOWN"}
                ) and (r.payload.get("strategy_id"), r.payload.get("strategy_version")) != IDENTITY:
                    protected.add(by_id[o.instrument_id].exchange_id)
            for owner in session.scalars(
                select(StrategyEvent).where(
                    StrategyEvent.account_id == account, StrategyEvent.state != "CLOSED"
                )
            ):
                if (owner.strategy_id, owner.strategy_version) != IDENTITY:
                    for key in [owner.instrument_id, *owner.data.get("instrument_ids", [])]:
                        if key in by_id:
                            protected.add(by_id[key].exchange_id)
            queued = list(
                session.scalars(
                    select(ExecutionCommand).where(
                        ExecutionCommand.account_id == account,
                        ExecutionCommand.strategy_id == IDENTITY[0],
                        ExecutionCommand.strategy_version == IDENTITY[1],
                        ExecutionCommand.state == "WAITING_ALLOCATION",
                    )
                )
            )
            for command in queued:
                if utc_time(command.expires_at) <= at and command.order_id is None:
                    command.state, command.result = (
                        "EXPIRED_BEFORE_ALLOCATION",
                        {"code": "COMMAND_EXPIRED"},
                    )
            waiting = {c.instrument_id for c in queued if c.state == "WAITING_ALLOCATION"}
            funds, projection = finances(session, account, p), family_projection(session, account)
            for exchange, directions in sorted(markets.items()):
                fyes = self.history(exchange, at)
                profiles.append({"exchange_id": exchange, **fyes})
                shadow_profiles.append(self.descriptive_windows(exchange, at))
                event = session.get(StrategyEvent, (account, *IDENTITY, exchange))
                if event is None:
                    base = next(iter(directions.values()))
                    event = StrategyEvent(
                        account_id=account,
                        strategy_id=IDENTITY[0],
                        strategy_version=IDENTITY[1],
                        event_key=exchange,
                        instrument_id=base.id,
                        state="CLOSED",
                        state_group="SINGLE_MARKET",
                        shock_group="GEOMETRIC_GRID",
                        direction_bucket=base.outcome,
                        future_fee_reserve=ZERO,
                        data={},
                    )
                    session.add(event)
                    session.flush()
                for inst in directions.values():
                    update_observation(event, inst, at, self.config.book_ttl_seconds)
                diagnosis = {
                    "exchange_id": exchange,
                    "historical_eligible": fyes.get("eligible", False),
                    "historical_reason": fyes.get("reason"),
                    "historical_count": fyes.get("count", 0),
                    "history_received_at": fyes.get("history_received_at"),
                    "protected": exchange in protected,
                    "books": {
                        side: {
                            "fresh_external_book": quote(i, at, self.config.book_ttl_seconds)
                            is not None,
                            "validated_at": i.book.get("validated_at"),
                            "sequence": i.book.get("sequence"),
                            "quality": i.quality,
                        }
                        for side, i in directions.items()
                    },
                    "e1": {},
                }
                diagnostics.append(diagnosis)
                if exchange in protected:
                    reasons["FOREIGN_ASSET_OR_LEGACY_RISK_PROTECTED"] += 1
                    continue
                current = event.state != "CLOSED"
                if current:
                    inst = by_id[event.instrument_id]
                elif fyes.get("eligible"):
                    yes = directions.get("YES")
                    yes_book = quote(yes, at, self.config.book_ttl_seconds) if yes else None
                    if not yes_book:
                        reasons["FRESH_DIRECTION_BOOK_REQUIRED"] += 1
                        continue
                    midpoint = (yes_book[0][0][0] + yes_book[1][0][0]) / 2
                    inst = directions.get("YES" if midpoint <= D(fyes["mu"]) else "NO")
                else:
                    reasons[fyes.get("reason") or "NO_PROFILE"] += 1
                    continue
                if inst is None:
                    reasons["DIRECTION_MAPPING_MISSING"] += 1
                    continue
                f = fyes if inst.outcome == "YES" else self.history(exchange, at, "NO")
                lots = [(lot, n) for lot, n in own if lot.instrument_id == inst.id]
                working = [(o, r) for o, r in active if o.instrument_id == inst.id]
                book = quote(inst, at, self.config.book_ttl_seconds)
                bid = book[0][0][0] if book else None
                plan = event.data.get("plan") if current else None
                if current and plan:
                    reason = risk_reason(event, f, bid, at, inst.tick)
                    if reason:
                        event.data = {**event.data, "risk_exit": reason}
                    # Actual favorable bid or first partial target fill starts exit.
                    if lots and (
                        event.data.get("exit_started")
                        or bid is not None
                        and bid >= D(plan["target"])
                    ):
                        event.data = {**event.data, "exit_started": True}
                risk = event.data.get("risk_exit") if current else None
                closing = current and (risk or event.data.get("exit_started"))
                for order, origin in working:
                    original = json.loads(origin.payload.get("note") or "{}")
                    review = "KEEP"
                    if risk:
                        review = risk
                    elif order.side == "BUY":
                        review = (
                            "EXIT_ONLY_OR_PAUSED"
                            if closing
                            or self.entries_disabled
                            or (self.root / "PAUSE_NEW_ENTRIES").exists()
                            else buy_review(
                                order, origin.payload, f, plan, book, at, inst.tick, p.order_seconds
                            )
                        )
                        if review == "KEEP" and not daily_gate(session, account, inst, at, p):
                            review = "DAILY_LOSS_LATCHED"
                    elif (
                        at - utc_time(origin.payload["created_at"])
                    ).total_seconds() >= order_seconds(original, p.order_seconds):
                        review = "MAX_ORDER_AGE"
                    prior_review = event.data.get("order_review", {})
                    if (
                        prior_review.get("action") != review
                        or prior_review.get("order_id") != order.id
                    ):
                        record = {
                            "action": review,
                            "order_id": order.id,
                            "at": at.isoformat(),
                            "version": EXECUTION_VERSION,
                            "bid": str(bid) if bid is not None else None,
                            "ask": str(book[1][0][0]) if book else None,
                            "target": plan["target"] if plan else None,
                            "profile_reason": f.get("reason"),
                            "book_validated_at": inst.book.get("validated_at"),
                        }
                        event.data = {**event.data, "order_review": record}
                        self.store.audit(session, account, "GRID_EXECUTION_REVIEW", record, at)
                    if review != "KEEP" and not order.cancel_state:
                        if review == "REPRICE_TO_BOUNDED_ACTIVE_WAIT_CANCEL":
                            deadline = min(
                                utc_time(origin.payload["created_at"])
                                + timedelta(seconds=order_seconds(original, p.order_seconds)),
                                at + timedelta(seconds=300),
                            )
                            event.data = {
                                **event.data,
                                "entry_replacement": {
                                    "source_order_id": order.id,
                                    "slot": original["slot"],
                                    "requested_at": at.isoformat(),
                                    "expires_at": deadline.isoformat(),
                                    "phase": "WAIT_CANCEL",
                                },
                            }
                        cancels[order.id] = review
                replacement = event.data.get("entry_replacement", {})
                replacing = replacement.get("phase") in {"WAIT_CANCEL", "READY"}
                replace_ready = False
                if replacing:
                    parent = session.get(Order, replacement["source_order_id"])
                    phase = replacement_state(parent, replacement, at)
                    if phase != replacement["phase"]:
                        self.store.audit(
                            session,
                            account,
                            "GRID_ENTRY_REPLACEMENT",
                            {
                                "source_order_id": parent.id,
                                "phase": phase,
                                "target": plan["target"] if plan else None,
                                "expires_at": replacement["expires_at"],
                            },
                            at,
                        )
                    event.data = {
                        **event.data,
                        "entry_replacement": {**replacement, "phase": phase},
                    }
                    if phase == "WAIT_CANCEL":
                        reasons["REPLACEMENT_WAIT_CANCEL_CONFIRMATION"] += 1
                        continue
                    if phase == "READY":
                        replace_ready = True
                    elif not lots and not working:
                        event.state = "CLOSED"
                        reasons["REPLACEMENT_" + phase] += 1
                        continue
                if inst.id in waiting:
                    reasons["COMMAND_PENDING"] += 1
                    continue
                note = {"exchange_id": exchange, "parameters": p.model_dump(mode="json")}
                if current and plan:
                    if event.holding_started_at:
                        note["holding_deadline"] = (
                            utc_time(event.holding_started_at)
                            + timedelta(seconds=plan["holding_seconds"])
                        ).isoformat()
                    note.update(
                        mu=plan["mu"],
                        width=plan["width"],
                        plan=plan,
                        cycle_id=event.data["cycle_id"],
                    )
                if risk:
                    if working:
                        reasons["RISK_WAIT_CANCEL_CONFIRMATION"] += 1
                    elif lots and book:
                        quantity = min(
                            sum((lot.quantity for lot, _ in lots), ZERO), book[0][0][1]
                        ).to_integral_value(rounding=ROUND_FLOOR)
                        if quantity:
                            candidates.append(
                                (
                                    0,
                                    self.make_intent(
                                        session,
                                        inst,
                                        quantity,
                                        bid,
                                        {**note, "kind": "RISK_EXIT", "reason": risk},
                                    ),
                                )
                            )
                    else:
                        reasons["RISK_EXIT_BOOK_PENDING"] += 1
                    continue
                if closing and any(o.side == "BUY" for o, _ in working):
                    reasons["EXIT_WAIT_BUY_CANCEL_CONFIRMATION"] += 1
                    continue
                if lots and not any(o.side == "SELL" for o, _ in working):
                    assert plan is not None
                    first = lots[0][0]
                    exit_evidence = executable_exit(book, first.quantity, D(plan["target"]))
                    candidates.append(
                        (
                            1,
                            self.make_intent(
                                session,
                                inst,
                                first.quantity,
                                D(plan["target"]),
                                {
                                    **note,
                                    "kind": "EXIT",
                                    "target": plan["target"],
                                    "execution": {
                                        "version": EXECUTION_VERSION,
                                        "action": "TARGET_LIMIT",
                                        "order_seconds": 300,
                                    },
                                    "exit_depth_observation": exit_evidence,
                                },
                            ),
                        )
                    )
                    continue
                if closing or self.entries_disabled or (self.root / "PAUSE_NEW_ENTRIES").exists():
                    reasons["EXIT_ONLY_OR_PAUSED"] += 1
                    continue
                if not f.get("eligible"):
                    reasons[f.get("reason") or "PROFILE_PENDING"] += 1
                    continue
                if D(f["drift"] or "0") <= D("-.01"):
                    reasons["ADVERSE_DRIFT"] += 1
                    continue
                if any(o.side == "BUY" for o, _ in working):
                    reasons["BUY_RESTING"] += 1
                    continue
                if not current:
                    rearm = event.data.get("rearm", {}).get(inst.outcome)
                    if rearm and not rearm.get("ready"):
                        reasons["WAIT_REAL_PRICE_REARM"] += 1
                        continue
                    try:
                        plan = frozen_plan(f, inst.tick, p)
                    except RiskRejected as exc:
                        reasons[exc.code] += 1
                        continue
                assert plan is not None
                motion = e1_diagnostics(
                    event.data.get("e1", {}).get(inst.outcome, []), at, inst.tick, plan
                )
                diagnosis["e1"][inst.outcome] = motion
                if not motion["eligible"]:
                    reasons["E1_MOVEMENT_OR_EXIT_REGION_NOT_OBSERVED"] += 1
                    continue
                if not daily_gate(session, account, inst, at, p):
                    reasons["DAILY_LOSS_LATCHED"] += 1
                    continue
                slot = (
                    replacement["slot"]
                    if replace_ready
                    else (
                        next(
                            (
                                j
                                for j in range(1, len(plan["levels"]) + 1)
                                if j not in event.data.get("used_slots", [])
                            ),
                            None,
                        )
                        if current
                        else 1
                    )
                )
                if slot is None:
                    reasons["ALL_ENABLED_SLOTS_FILLED"] += 1
                    continue
                execution = entry_choice(book, plan, slot, inst.tick)
                if execution["action"] == "SKIP":
                    reasons[execution["reason"]] += 1
                    continue
                if replace_ready and execution["action"] != "ACTIVE_LIMIT":
                    reasons["REPLACEMENT_WAIT_BOUNDED_ACTIVE_PRICE"] += 1
                    continue
                price = D(execution["price"])
                q = size_order(
                    plan,
                    slot,
                    price,
                    projection,
                    exchange,
                    funds["available"],
                    capacity(
                        inst,
                        plan,
                        at,
                        self.config.book_ttl_seconds,
                        price if execution["action"] == "ACTIVE_LIMIT" else None,
                    ),
                    p,
                )
                if q <= 0:
                    reasons["RISK_CASH_CAPACITY_OR_BOOK_LIMIT"] += 1
                    continue
                if replace_ready:
                    q = min(q, parent.quantity)
                    note = {
                        **note,
                        "replacement_of": parent.id,
                        "replacement_deadline": replacement["expires_at"],
                    }
                frozen = event.data["frozen_profile"] if current else f
                opportunity = event.data.get("opportunity", {})
                if opportunity.get("outcome") != inst.outcome or opportunity.get("ended"):
                    opportunity = {
                        "id": content_hash([exchange, inst.outcome, at.isoformat()]),
                        "outcome": inst.outcome,
                        "started_at": at.isoformat(),
                        "ended": False,
                        "reference": plan["target"],
                    }
                event.data = {**event.data, "opportunity": opportunity}
                execution.update(
                    book_validated_at=inst.book["validated_at"],
                    book_received_at=inst.book_at.isoformat(),
                    book_version=inst.book.get("sequence"),
                    book_hash=content_hash(inst.book),
                    external_bid=str(book[0][0][0]),
                    external_ask=str(book[1][0][0]),
                    capacity=str(
                        capacity(
                            inst,
                            plan,
                            at,
                            self.config.book_ttl_seconds,
                            price if execution["action"] == "ACTIVE_LIMIT" else None,
                        )
                    ),
                )
                candidates.append(
                    (
                        1 if replace_ready else 2 + abs(D(f["last"]) - price),
                        self.make_intent(
                            session,
                            inst,
                            q,
                            price,
                            {
                                **note,
                                "kind": "ENTRY",
                                "mu": plan["mu"],
                                "width": plan["width"],
                                "slot": slot,
                                "level": str(price),
                                "target": plan["target"],
                                "plan": plan,
                                "cycle_id": plan_key(plan),
                                "profile": f,
                                "frozen_profile": frozen,
                                "execution": execution,
                                "opportunity_id": opportunity["id"],
                                "execution_comparison": {
                                    "forced_passive_5m_price": str(
                                        min(D(plan["levels"][slot - 1]), book[1][0][0] - inst.tick)
                                    ),
                                    "passive_max_age_seconds": [300, 900],
                                    "active_limit_ceiling": execution["ceiling"],
                                    "counterfactual_fills": "NOT_OBSERVED",
                                    "counterfactual_profit": "NOT_ESTIMATED",
                                },
                            },
                        ),
                    )
                )
            self.status = {
                "at": at.isoformat(),
                "strategy": list(IDENTITY),
                "status": "SCANNING",
                "market_count": len(markets),
                "profile_eligible": sum(x.get("eligible", False) for x in profiles),
                "candidate_count": len(candidates),
                "active_orders": len(active),
                "cash": {k: str(v) for k, v in funds.items()},
                "committed_scenario_risk": str(sum((m["risk"] for m in projection.values()), ZERO)),
                "reasons": dict(reasons),
                "parameters": p.model_dump(mode="json"),
                "fees": "0",
                "forward_validation": "IN_PROGRESS_NOT_PROFIT_PROVEN",
                "shape_selection": "OPERATOR_FIXED_ALPHA075_K1_H60_STRESS2_NOT_OPTIMIZED",
                "execution_version": EXECUTION_VERSION,
                "execution_counts": dict(
                    Counter(
                        json.loads(i.note)["execution"]["action"]
                        for _, i in candidates
                        if json.loads(i.note).get("execution")
                    )
                ),
            }
        for order_id in sorted(cancels):
            self.queue.cancel(order_id, at)
        if len(waiting) < 2 and candidates:
            self.status["last_command"] = self.queue.publish(min(candidates, key=lambda x: x[0])[1])
        atomic_json(self.root / "STATUS.json", self.status)
        atomic_json(self.root / "RANGE_PROFILES.json", {"at": at.isoformat(), "markets": profiles})
        atomic_json(
            self.root / "ENTRY_DIAGNOSTICS.json", {"at": at.isoformat(), "markets": diagnostics}
        )
        atomic_json(
            self.root / "RANGE_PROFILES_SHADOW.json",
            {"at": at.isoformat(), "markets": shadow_profiles},
        )
