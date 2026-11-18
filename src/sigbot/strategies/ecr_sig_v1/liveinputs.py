"""Actual REST samples -> closed minute windows -> the unchanged ECR signal core.

No interpolation, no reconstructed past quotes, no order side effects. Data
coverage does not depend on fee/account approval; those are evaluated separately.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from decimal import Decimal

from sigbot.strategies.ecr_sig_v1 import core


def fresh(book: dict, now: datetime) -> bool:
    source = book.get("source_at")
    if book.get("source_time_status") == "MISSING_OFFICIAL_AS_OF":
        return False
    if not book.get("bids") or not book.get("asks"):
        return False
    source_at = datetime.fromisoformat(source) if source else None
    received = datetime.fromisoformat(book["received_at"])
    started = datetime.fromisoformat(book.get("request_started_at") or book["received_at"])
    if (
        received.tzinfo is None
        or started.tzinfo is None
        or (source_at is not None and source_at.tzinfo is None)
    ):
        return False
    return bool(
        started <= received <= now
        and (source_at is None or source_at <= received)
        and 0 <= (now - started).total_seconds() <= 5
        and Decimal(str(book["bid"])) < Decimal(str(book["ask"]))
    )


def book_input(book: dict) -> core.Book:
    return core.Book(
        tuple(core.Level(Decimal(x["price"]), Decimal(x["size"])) for x in book["bids"]),
        tuple(core.Level(Decimal(x["price"]), Decimal(x["size"])) for x in book["asks"]),
        int(datetime.fromisoformat(book["received_at"]).timestamp()),
        True,
    )


class MinuteSeries:
    def __init__(self):
        self.samples: deque = deque(maxlen=180)
        self.minutes: deque = deque(maxlen=65)
        self.last_closed: int | None = None
        self.identity: tuple | None = None

    def add(self, observation: dict, binding: tuple) -> list[dict]:
        if self.identity != binding:
            self.samples.clear()
            self.minutes.clear()
            self.last_closed = None
            self.identity = binding
        self.samples.append(observation)
        current = int(observation["at"] // 60) * 60
        start = self.last_closed + 60 if self.last_closed is not None else current - 60
        closed = []
        # Explicitly emit gaps, including a multi-minute timeout.
        for bucket in range(max(start, current - 65 * 60), current, 60):
            samples = [s for s in self.samples if bucket <= s["at"] < bucket + 60]
            times = sorted({float(s["at"]) for s in samples})
            boundaries = [float(bucket), *times, float(bucket + 60)]
            gap = max((b - a for a, b in zip(boundaries, boundaries[1:], strict=False)), default=60)
            ok = len(times) >= 3 and gap <= 25 and all(s["strategy_ready"] for s in samples)
            latest = max(samples, key=lambda s: s["at"]) if samples else None
            quote = None
            if ok and latest:
                quote = core.Observation(
                    bucket + 60,
                    Decimal(str(latest["sig"]["bid"])),
                    Decimal(str(latest["sig"]["ask"])),
                    tuple(
                        (b["operator"], Decimal(str(b["bid"])), Decimal(str(b["ask"])))
                        for b in latest["external"]
                    ),
                    True,
                )
                quote.validate()
            minute = {
                "at": datetime.fromtimestamp(bucket + 60, timezone.utc).isoformat(),
                "samples": len(times),
                "maximum_sample_gap_seconds": gap,
                "sampling_complete": len(times) >= 3 and gap <= 25,
                "strategy_complete": ok,
                "no_imputation": True,
                "available_at": datetime.fromtimestamp(observation["at"], timezone.utc).isoformat(),
                "quote": quote,
            }
            self.minutes.append(minute)
            self.last_closed = bucket
            closed.append(minute)
        return closed

    def window(
        self, now: datetime
    ) -> tuple[core.Observation | None, tuple[core.Observation, ...], list[str]]:
        good = [m["quote"] for m in self.minutes if m["strategy_complete"]]
        if not good:
            return None, (), ["NO_COMPLETE_PRICE_MINUTE"]
        last = good[-1]
        history = [last]
        for item in reversed(good[:-1]):
            if history[0].at - item.at != 60:
                break
            history.insert(0, item)
        base = next((x for x in reversed(good) if 1800 <= last.at - x.at <= 2100), None)
        reasons = []
        if base is None:
            reasons.append("BASELINE_30_35_MINUTES_MISSING")
        if not 0 <= now.timestamp() - last.at <= 10:
            reasons.append("LATEST_COMPLETED_MINUTE_OLDER_THAN_10_SECONDS")
        if len(history) < 5:
            reasons.append("M_WINDOW_FEWER_THAN_5_CONTIGUOUS_MINUTES")
        if len(history) < 15:
            reasons.append("P_WINDOW_FEWER_THAN_15_CONTIGUOUS_MINUTES")
        return base, tuple(history), reasons


def evaluate(
    row: dict,
    pair: dict,
    observation: dict,
    series: MinuteSeries,
    fee: dict,
    now: datetime,
    catalyst: dict | None = None,
) -> dict:
    base, history, window_gaps = series.window(now)
    fee_ready = (
        fee["status"] == "VERIFIED"
        or (
            fee["status"] == "EMPIRICAL_ACCEPTED"
            and fee.get("model_kind") == "EMPIRICAL_ACTUAL_TRADE_CALIBRATION"
            and fee.get("accepted_by_operator") is True
            and fee.get("official_future_fee_schedule_verified") is False
        )
    ) and all(fee.get(name) is not None for name in ("buy_fee_bound", "sell_fee_bound"))
    reasons = list(window_gaps)
    if not pair["predicate_match"] or not pair["unique_operator_match"]:
        reasons.append(pair["reason"])
    if not pair["settlement_equivalent"]:
        reasons.extend(pair["settlement_gaps"])
    if not fee_ready:
        reasons.append("SIG_TRADING_FEES_UNKNOWN")
    if not all(fresh(b, now) for b in [observation["sig"], *observation["external"]]):
        reasons.append("CURRENT_BOOK_READ_MISSING_STALE_OR_TIMEOUT")
    event = pair["event"]
    result = {
        "api_market_id": row["api_market_id"],
        "event_key": event["event_key"] if event else None,
        "layer": "REAL_READ",
        "core_called": False,
        "qualified": False,
        "reasons": reasons,
        "complete_contiguous_minutes": len(history),
        "baseline_present": base is not None,
        "real_orders": 0,
        "authorized_for_live": False,
        "fee_adjusted_gap": None,
        "fee_model_kind": fee.get("model_kind", fee["status"]),
        "official_future_fee_schedule_verified": fee["status"] == "VERIFIED",
        "expected_pnl": None,
    }
    if (
        not base
        or not event
        or not history
        or not 0 <= now.timestamp() - history[-1].at <= 10
        or not observation["external"]
        or not all(fresh(b, now) for b in [observation["sig"], *observation["external"]])
        or not all(
            b.get("received_at") and b["bids"] and b["asks"]
            for b in [observation["sig"], *observation["external"]]
        )
    ):
        return result
    # Fee sentinels below cannot participate in arithmetic: fee_verified is false
    # and core.signal rejects before pricing. Published unknown bounds remain null.
    candidate = core.Candidate(
        key="ECR-" + row["api_market_id"],
        event_key=event["event_key"],
        state=event["state"],
        shock_group="ELECTION-" + event["state"],
        direction_bucket=event["party"],
        office=event["office"],
        side="YES",
        sig_book=book_input(observation["sig"]),
        external_books=tuple((b["operator"], book_input(b)) for b in observation["external"]),
        baseline=base,
        history=history,
        rules_reviewed=pair["predicate_match"],
        mapping_reviewed=pair["settlement_equivalent"],
        rules_unchanged=True,
        fee_verified=fee_ready,
        buy_fee_bound=Decimal(fee["buy_fee_bound"])
        if fee["buy_fee_bound"] is not None
        else core.ZERO,
        sell_fee_bound=Decimal(fee["sell_fee_bound"])
        if fee["sell_fee_bound"] is not None
        else core.ZERO,
        lot=Decimal(1),
        min_qty=Decimal(1),
        price_tick=Decimal(".005"),
        tradable_until=int(datetime.fromisoformat(row["settlement_date"]).timestamp()),
        catalyst_verified=bool(catalyst),
        catalyst_at=catalyst.get("at") if catalyst else None,
        catalyst_event_key=catalyst.get("event_key") if catalyst else None,
    )
    result["core_called"] = True
    try:
        signal = core.signal(candidate, int(now.timestamp()))
        result.update(qualified=True, route=signal.route, reasons=[])
    except core.Reject as exc:
        result["core_rejection"] = str(exc)
    return result
