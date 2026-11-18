"""Lightweight real-book observations, independent of expensive quote fitting."""

from contextlib import nullcontext
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from time import perf_counter, thread_time
from types import SimpleNamespace
from typing import cast as typed_cast

from sqlalchemy import (
    JSON,
    Boolean,
    Table,
    Text,
    bindparam,
    case,
    cast,
    column,
    func,
    or_,
    select,
    values,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import load_only

from sigbot.domain import TERMINAL, utc_time
from sigbot.errors import RiskRejected, SchemaMismatch
from sigbot.storage import Account, Instrument, IntentRow, Order
from sigbot.strategies.mm_sig_v1.core import external_best
from sigbot.strategies.mm_sig_v1.data import normalized_book
from sigbot.strategies.mm_sig_v1.live.analytics import record


def checkpoint(
    runtime,
    review,
    book,
    current,
    timestamp,
    *,
    mirror=True,
    session=None,
    serialized=None,
    reference_updates=None,
    previous_reference=None,
):
    """An actual independent safety observation, never a refreshed old quote."""
    own_session = session is None
    with runtime.live_store.transaction() if own_session else nullcontext(session) as session:
        inst = session.get(
            Instrument, review["instrument_id"], with_for_update=True if own_session else None
        )
        if inst is None:
            if mirror:
                raise SchemaMismatch("MM_REFERENCE_INSTRUMENT_NOT_STAGED")
            return  # A read-only NO signal never invents a trading instrument.
        previous = (
            inst.mapping_evidence.get("mm13_reference", {})
            if previous_reference is None
            else previous_reference
        )
        binding = [review["rules_hash"], review["mapping_hash"]]
        if (
            previous.get("binding") == binding
            and previous.get("at")
            and timestamp < utc_time(previous["at"])
        ):
            return  # A slow sampler must not replace a later REST/account proof.
        if (
            not current
            and previous.get("current")
            and previous.get("source")
            == "AUTHORITATIVE_REST_BOOK_AND_COMPLETE_OWN_ORDER_REVALIDATION"
            and previous.get("binding") == binding
            and 0
            <= (timestamp - utc_time(previous["at"])).total_seconds()
            <= runtime.settings.book_ttl_seconds
        ):
            # A missing shared stream cannot invalidate a newer full REST baseline.
            # Keep its original timestamp; account/unknown-order gates are checked separately.
            return
        previous_current = bool(
            previous.get("current")
            and previous.get("binding") == binding
            and 0
            < (timestamp - utc_time(previous["at"])).total_seconds()
            <= runtime.settings.book_ttl_seconds
        )
        cooldown = previous.get("cooldown_until")
        mid = None
        book_update = None
        if current:
            bid, ask = external_best(book)
            mid = (bid + ask) / 2
            if (
                previous_current
                and previous.get("mid") is not None
                and abs(mid - Decimal(previous["mid"]))
                >= Decimal(review["tick"]) * runtime.settings.jump_ticks
            ):
                cooldown = (
                    timestamp + timedelta(seconds=runtime.settings.jump_cooldown_seconds)
                ).isoformat()
            if serialized is None:
                serialized = book.model_dump(mode="json", exclude={"received_at", "quality"})
            book_update = {
                **serialized,
                "validated_at": timestamp.isoformat(),
                "continuity_verified": True,
            }
            if reference_updates is None:
                inst.book = book_update
                inst.book_at, inst.quality = book.received_at, book.quality
        reference = {
            "at": timestamp.isoformat(),
            "binding": binding,
            "current": current,
            "ends_at": (runtime.account_data or {}).get("cup", {}).get("endDate"),
            "consistent_checks": min(2, previous.get("consistent_checks", 0) + 1)
            if current and previous_current
            else int(current),
            "mid": str(mid) if mid is not None else None,
            "cooldown_until": cooldown,
            "source": "INDEPENDENT_VERIFIED_BOOK_AND_ACCOUNT",
            "quote_retimestamped": False,
        }
        if reference_updates is not None:
            reference_updates.append(
                {
                    "_id": inst.id,
                    "_reference": reference,
                    "_has_book": current,
                    "_book": book_update,
                    "_book_at": book.received_at if current else None,
                    "_quality": book.quality if current else None,
                }
            )
        else:
            inst.mapping_evidence = {**inst.mapping_evidence, "mm13_reference": reference}

    if mirror:
        from sigbot.strategies.mm_sig_v1.data import outcome_book

        no_review = {
            **review,
            "instrument_id": review["instrument_id"].removesuffix("-YES") + "-NO",
        }
        checkpoint(
            runtime,
            no_review,
            outcome_book(book, "NO") if book else None,
            current,
            timestamp,
            mirror=False,
            session=None if own_session else session,
        )


def sample(runtime, timestamp):
    phases = {}
    phase_wall, phase_cpu = perf_counter(), thread_time()

    def measured(name):
        nonlocal phase_wall, phase_cpu
        wall, cpu = perf_counter(), thread_time()
        phases[name] = {"wall_seconds": wall - phase_wall, "cpu_seconds": cpu - phase_cpu}
        phase_wall, phase_cpu = wall, cpu

    with runtime.lock:
        data = deepcopy(runtime.account_data)
        account_at, result = runtime.account_at, runtime.account_result
        reviews = deepcopy(runtime.reviews.copy())
        tracked = list(runtime.tracking_exchanges)
        # Quote evaluation publishes keys while this worker copies values.
        # Snapshot the mapping before deepcopy can yield the GIL to that writer.
        quotes = deepcopy(runtime.state.get("quotes", {}).copy())
        rejected = deepcopy(runtime.state.get("admission_rejected", {}).copy())
    measured("state_capture")
    books, _, _ = runtime.collector.snapshot()
    measured("shared_source")
    current = bool(
        data
        and result
        and result["status"] == "PASS"
        and account_at
        and 0
        <= (timestamp - utc_time(account_at)).total_seconds()
        <= runtime.settings.account_ttl_seconds
    )
    own = {str(o["id"]): deepcopy(o) for o in (data or {}).get("orders", []) if o["open"]}
    with runtime.live_store.transaction() as session:
        account = session.get(Account, runtime.aid)
        order_rows = list(
            session.execute(
                select(Order, IntentRow)
                .outerjoin(IntentRow, Order.intent_id == IntentRow.id)
                .where(
                    Order.account_id == runtime.aid,
                    or_(
                        Order.state.not_in(TERMINAL),
                        Order.request_state.in_(["SENDING", "UNKNOWN"]),
                        Order.cancel_state == "CANCEL_UNKNOWN",
                        Order.remote_id.in_(own),
                    ),
                )
            )
        )
        known = {str(o.remote_id) for o, _ in order_rows if o.remote_id}
        current = (
            current and account is not None and account.lifecycle == "HEALTHY" and set(own) <= known
        )
        from sigbot.uncertainty import scope_blockers, scopes

        pending = scopes(session, account, [o for o, _ in order_rows]) if account else []
        blocked = {
            eid
            for eid, review in reviews.items()
            if scope_blockers(
                pending,
                SimpleNamespace(exchange_id=eid, event_group=review.get("event_key")),
            )
        }
        for order, intent in order_rows:
            if order.remote_id and order.state in TERMINAL and order.request_state == "RESOLVED":
                own.pop(str(order.remote_id), None)
            elif order.remote_id and order.request_state == "ACKNOWLEDGED":
                inst = session.get(Instrument, order.instrument_id)
                if inst is None or intent is None:
                    current = False
                    continue
                own[str(order.remote_id)] = {
                    "id": order.remote_id,
                    "exchangeId": inst.exchange_id,
                    "side": inst.outcome.lower(),
                    "action": order.side.lower(),
                    "priceLimit": order.price,
                    "quantity": order.quantity - order.filled,
                    "open": True,
                    "expirationDate": None,
                    "createdAt": intent.payload["created_at"],
                }
    measured("account_orders")
    # One account/order read supplies every outcome; no query per empty market.
    order_groups: dict[str, list[tuple[Order, IntentRow | None]]] = {}
    for order, intent in order_rows:
        order_groups.setdefault(order.instrument_id, []).append((order, intent))
    prepared = {}
    cache = getattr(runtime, "_reference_cache", {})
    cache = {eid: row for eid, row in cache.items() if eid in tracked}
    runtime._reference_cache = cache
    cache_hits = 0
    for eid in tracked:
        review = reviews.get(eid)
        if review is None:
            continue
        raw = books.get(eid)
        healthy = bool(
            current
            and eid not in blocked
            and eid not in rejected
            and raw
            and raw.get("validation_received_at")
            and raw["marketId"] == review["market_id"]
            and raw["exchangeId"] == eid
            and 0
            <= (timestamp - utc_time(raw["validation_received_at"])).total_seconds()
            <= runtime.settings.tracked_book_refresh_seconds + runtime.settings.book_ttl_seconds
            and (not raw.get("nextExpiryAt") or utc_time(raw["nextExpiryAt"]) > timestamp)
        )
        book = None
        if healthy:
            own_orders = [o for o in own.values() if o["exchangeId"] == eid]
            binding = [review["rules_hash"], review["mapping_hash"]]
            cached = cache.get(eid)
            if (
                cached
                and cached["raw"] == raw
                and cached["own"] == own_orders
                and cached["binding"] == binding
            ):
                book = cached["books"]["YES"]
                cache_hits += 1
            else:
                try:
                    book = normalized_book(
                        raw,
                        own_orders,
                        utc_time(raw["received_at"]),
                        previous=(cached["raw"], cached["own"], cached["books"]["YES"])
                        if cached
                        else None,
                    )
                    external_best(book)
                    from sigbot.strategies.mm_sig_v1.data import outcome_book

                    sides = {"YES": book, "NO": outcome_book(book, "NO")}
                    cache[eid] = {
                        "raw": deepcopy(raw),
                        "own": deepcopy(own_orders),
                        "binding": binding,
                        "books": sides,
                        "serialized": {
                            side: value.model_dump(mode="json", exclude={"received_at", "quality"})
                            for side, value in sides.items()
                        },
                    }
                except (SchemaMismatch, RiskRejected):
                    book = None
        # Retain validated numeric levels across a temporary input gap. They
        # cannot make this round usable; all current-input gates run above.
        if book is None and healthy:
            cache.pop(eid, None)
        prepared[eid] = book
    ids = {
        (
            review["instrument_id"]
            if outcome == "YES"
            else review["instrument_id"].removesuffix("-YES") + "-NO"
        )
        for eid, review in reviews.items()
        if eid in prepared
        for outcome in ("YES", "NO")
    }
    measured("normalize_books")
    skipped = set()
    # Lock without waiting on the gateway, then release before observation writes.
    # SKIP LOCKED prevents cycles with gateway transactions holding account/order locks.
    with runtime.live_store.transaction() as session:
        present = set(session.scalars(select(Instrument.id).where(Instrument.id.in_(ids))))
        query = (
            select(Instrument)
            .where(Instrument.id.in_(ids))
            .order_by(Instrument.id)
            .with_for_update(skip_locked=True)
        )
        native = runtime.live_store.engine.dialect.name == "postgresql"
        old_references = {}
        if native:
            # The large policy caches and last full books are not reference inputs.
            # Keep the actual reference from the same locked row, and update only
            # its JSON subtree below so concurrent policy metadata is preserved.
            rows = session.execute(
                query.add_columns(Instrument.mapping_evidence["mm13_reference"]).options(
                    load_only(Instrument.id)
                )
            ).all()
            locked = {inst.id: inst for inst, _ in rows}
            old_references = {inst.id: value or {} for inst, value in rows}
        else:
            locked = {inst.id: inst for inst in session.scalars(query)}
        skipped = present - locked.keys()
        reference_updates: list[dict] | None = [] if native else None
        for eid, book in prepared.items():
            review = reviews[eid]
            if review["instrument_id"] not in present:
                raise SchemaMismatch("MM_REFERENCE_INSTRUMENT_NOT_STAGED")
            for outcome in ("YES", "NO"):
                iid = (
                    review["instrument_id"]
                    if outcome == "YES"
                    else review["instrument_id"].removesuffix("-YES") + "-NO"
                )
                if iid not in locked:
                    continue
                checkpoint(
                    runtime,
                    {**review, "instrument_id": iid},
                    cache[eid]["books"][outcome] if book else None,
                    book is not None
                    and not bool(
                        runtime.state.get("account_last_error")
                        or runtime.state.get("market_read_error_blocks_quotes")
                        or getattr(runtime, "fee_reason", None)
                    ),
                    timestamp,
                    mirror=False,
                    session=session,
                    serialized=cache[eid]["serialized"][outcome] if book else None,
                    reference_updates=reference_updates,
                    previous_reference=old_references.get(iid) if native else None,
                )
        if reference_updates:
            session.flush()
            table = typed_cast(Table, Instrument.__table__)
            updates = (
                values(
                    column("_id", table.c.id.type),
                    column("_reference", JSONB),
                    column("_has_book", Boolean),
                    column("_book", table.c.book.type),
                    column("_book_at", table.c.book_at.type),
                    column("_quality", table.c.quality.type),
                )
                .data(
                    [
                        tuple(
                            row[key]
                            for key in (
                                "_id",
                                "_reference",
                                "_has_book",
                                "_book",
                                "_book_at",
                                "_quality",
                            )
                        )
                        for row in reference_updates
                    ]
                )
                .alias("references")
            )
            session.execute(
                table.update()
                .where(table.c.id == updates.c._id)
                .values(
                    book=case((updates.c._has_book, updates.c._book), else_=table.c.book),
                    book_at=case(
                        (updates.c._has_book, cast(updates.c._book_at, table.c.book_at.type)),
                        else_=table.c.book_at,
                    ),
                    quality=case((updates.c._has_book, updates.c._quality), else_=table.c.quality),
                    mapping_evidence=cast(
                        func.jsonb_set(
                            cast(table.c.mapping_evidence, JSONB),
                            bindparam("_path", value=["mm13_reference"], type_=ARRAY(Text)),
                            updates.c._reference,
                            True,
                        ),
                        JSON,
                    ),
                )
            )
    measured("persist_checkpoints")
    count, gaps = 0, 0
    exchanges: dict[str, dict] = {}
    with runtime.live_store.transaction() as session, session.no_autoflush:
        for eid, book in prepared.items():
            review = reviews[eid]
            if book is None:
                exchanges[eid] = {"current": False}
                for outcome in ("YES", "NO"):
                    runtime.live_store.audit(
                        session,
                        runtime.aid,
                        "MM13_OBSERVATION",
                        {
                            "instrument_id": review["instrument_id"]
                            if outcome == "YES"
                            else review["instrument_id"].removesuffix("-YES") + "-NO",
                            "signal_binding": [review["rules_hash"], review["mapping_hash"]],
                            "price_reference_usable": False,
                            "reference_usable": False,
                            "quote_usable": False,
                            "eligible_orders": {},
                            "source": "INDEPENDENT_BOOK_REFERENCE_SAMPLER",
                            "reason": "REFERENCE_BOOK_OR_ACCOUNT_UNCONFIRMED",
                            "synthetic": runtime.live_store.namespace != "live",
                            "layer": "REAL_READ"
                            if runtime.live_store.namespace == "live"
                            else "MOCK",
                        },
                        timestamp,
                    )
                gaps += 1
                continue
            assert book is not None
            outcomes = ("YES", "NO")
            for outcome in outcomes:
                quote_key = eid if outcome == "YES" else eid + ":NO"
                side_review = {
                    **review,
                    "instrument_id": review["instrument_id"]
                    if outcome == "YES"
                    else review["instrument_id"].removesuffix("-YES") + "-NO",
                }
                side_book = cache[eid]["books"][outcome]
                latest = quotes.get(quote_key, {})
                fresh_quote = bool(
                    latest.get("validated_at")
                    and 0
                    <= (timestamp - utc_time(latest["validated_at"])).total_seconds()
                    <= runtime.settings.book_ttl_seconds
                    and not latest.get("blocking_reasons")
                    and latest.get("rules_hash") == side_review["rules_hash"]
                    and latest.get("mapping_hash") == side_review["mapping_hash"]
                )
                # A price observation is independent of an order recommendation. A stale
                # recommendation never creates eligible order exposure or a fake fill.
                quote = {k: latest.get(k) for k in ["bid", "ask", "buy_guard", "sell_guard"]}
                quote["buy_guard"] = quote["buy_guard"] or "0"
                quote["sell_guard"] = quote["sell_guard"] or "1"
                quote.update(
                    reference_usable=True,
                    tick=side_review["tick"],
                    rules_hash=side_review["rules_hash"],
                    mapping_hash=side_review["mapping_hash"],
                    source="INDEPENDENT_BOOK_REFERENCE_SAMPLER",
                )
                record(
                    runtime.live_store,
                    runtime.aid,
                    side_review["instrument_id"],
                    side_book,
                    quote,
                    timestamp,
                    price_reference_usable=True,
                    reference_usable=fresh_quote,
                    quote_usable=fresh_quote,
                    order_exposure_usable=True,
                    activity=latest.get("activity_status", "UNKNOWN"),
                    session=session,
                    orders=order_groups.get(side_review["instrument_id"], []),
                )
                exchanges[quote_key] = {
                    "current": side_review["instrument_id"] not in skipped,
                    "rules_hash": review["rules_hash"],
                    "mapping_hash": review["mapping_hash"],
                    "sequence": book.sequence,
                }
            count += 1
    measured("persist_observations")
    return {
        "normalized_cache_hits": cache_hits,
        "phase_timings": phases,
        "at": timestamp.isoformat(),
        "usable": count,
        "gaps": gaps,
        "source": "ACTUAL_VERSIONED_BOOKS",
        "new_http_requests": 0,
        "checkpoint_skipped_locked": sorted(skipped),
        "exchanges": exchanges,
    }
