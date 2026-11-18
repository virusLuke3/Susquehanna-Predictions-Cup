"""Bounded account reads plus existing shared books; no trading or ledger writes."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy import select, text

from sigbot.domain import TERMINAL, content_hash, utc_time
from sigbot.errors import AuthorizationRequired, SchemaMismatch
from sigbot.readcheck import ReadCheck, load_key
from sigbot.storage import Account, Instrument, Order
from sigbot.strategies.mm_sig_v1.data import normalized_book, outcome_book
from sigbot.strategies.semantic_sig_v1.registry import write_json


def local_order_check(store, account_id, tournament_id, official_orders):
    """Read common OMS uncertainties; do not replace current official order quantities."""
    blocked = set()
    with store.transaction() as session:
        if store.engine.dialect.name == "postgresql":
            session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        account = session.get(Account, account_id)
        if account is None or account.tournament != tournament_id:
            raise SchemaMismatch("SEMANTIC_OMS_ACCOUNT_SCOPE_MISMATCH")
        orders = list(session.scalars(select(Order).where(Order.account_id == account_id)))
        remote = {str(o["id"]): o for o in official_orders}
        for order in orders:
            if order.state in TERMINAL and order.request_state == "RESOLVED":
                continue
            inst = session.get(Instrument, order.instrument_id)
            if inst is None or inst.tournament != tournament_id or not inst.exchange_id:
                raise SchemaMismatch("SEMANTIC_OMS_ORDER_MAPPING_UNKNOWN")
            observed = remote.get(str(order.remote_id)) if order.remote_id else None
            uncertain = (
                order.request_state in {"SENDING", "UNKNOWN"}
                or order.cancel_state in {"CANCEL_REQUESTED", "CANCEL_UNKNOWN"}
                or order.request_state == "ACKNOWLEDGED"
                and observed is None
            )
            if observed is not None and (
                observed["exchangeId"] != inst.exchange_id
                or observed["side"] != inst.outcome.lower()
                or observed["action"] != order.side.lower()
                or Decimal(str(observed["quantity"])) != order.quantity - order.filled
            ):
                uncertain = True
            if uncertain:
                blocked.add(inst.exchange_id)
        return {
            "blocked_exchange_ids": sorted(blocked),
            "oms_lifecycle": account.lifecycle,
            "unmanaged_official_orders": sum(
                str(o["id"]) not in {str(r.remote_id) for r in orders} for o in official_orders
            ),
            "account_execution_approval": False,
        }


def apply_owned_depth(books, orders, *, at, blocked_exchange_ids=()):
    """Reuse MM ownership subtraction and within-contract NO conversion exactly."""
    normalized, failures = {}, {}
    for eid, raw in books.items():
        if eid in blocked_exchange_ids:
            failures[eid] = "SEMANTIC_OMS_ORDER_STATE_UNCERTAIN"
            continue
        if raw.get("quality") != "HEALTHY":
            failures[eid] = "BOOK_DATA_GAP"
            continue
        try:
            owned = normalized_book(raw, orders, at)
            no = outcome_book(owned, "NO")
        except SchemaMismatch as exc:
            failures[eid] = exc.code
            continue
        except ValidationError:
            failures[eid] = "SEMANTIC_BOOK_SCHEMA_INVALID"
            continue
        normalized[eid] = {
            **raw,
            "own_orders_known": True,
            "no_asks": [
                {
                    "price": str(level.price),
                    "quantity": str(level.quantity - level.own_quantity),
                    "own": False,
                }
                for level in no.asks
                if level.quantity > level.own_quantity
            ],
            "own_order_exclusion": "TWO_CONSISTENT_OFFICIAL_READS_AND_COMMON_OMS_NOT_ATOMIC",
        }
    return normalized, failures


def capture_account_books(args, markets, tid, account_hash):
    if not args.authorize_read:
        raise AuthorizationRequired("SEMANTIC_EXPLICIT_ACCOUNT_READ_REQUIRED")
    from sigbot.strategies.mm_sig_v1.live.database import open_store
    from sigbot.strategies.semantic_sig_v1.research import shared_snapshot

    # The application loads authentication only for already documented official GETs.
    reads = ReadCheck(
        load_key(args.env_file),
        tournament_slug="midterm-elections",
        root=Path("runtime/readonly"),
        max_requests=args.max_requests,
        seconds=args.seconds,
        contract_file=args.account_contract,
    )
    store = None
    reads.reader.transport.charge = lambda category, weight: reads.budget.charge(
        category, weight, wait=True
    )
    read_report: dict = {"status": "INCOMPLETE", "real_orders": 0, "real_cancels": 0}
    try:
        reads.reader.config.expected_tournament_id = tid
        reads.reader.config.expected_tournament_slug = reads.slug
        identity = reads.call("account_identity", query={"tournamentId": tid})
        aid = identity["id"]
        if hashlib.sha256(aid.encode()).hexdigest() != account_hash:
            raise SchemaMismatch("SEMANTIC_OFFICIAL_ACCOUNT_IDENTITY_MISMATCH")
        reads.reader.config.expected_account_id = aid
        reads.reader.config.expected_tournament_id = tid
        before, _ = reads.pages(
            "open_orders", query={"tournamentId": tid, "status": "open", "limit": 200}
        )
        after, _ = reads.pages(
            "open_orders", query={"tournamentId": tid, "status": "open", "limit": 200}
        )
        # Read the current shared source after potentially slow account GETs;
        # never restamp an old book captured before waiting for the shared budget.
        books, quality, manifest = shared_snapshot(
            args.shared_source, markets, tid, account_hash, datetime.now(timezone.utc), args.max_age
        )
        if any(not o["open"] or o["tournamentId"] != tid for o in [*before, *after]):
            raise SchemaMismatch("SEMANTIC_OPEN_ORDER_SCOPE_INVALID")
        stable = content_hash(sorted(before, key=lambda o: str(o["id"]))) == content_hash(
            sorted(after, key=lambda o: str(o["id"]))
        )
        store = open_store("live")
        context = local_order_check(store, aid, tid, after)
        latest_read = reads.requests[-1].get("received_at") if reads.requests else None
        current = bool(
            latest_read
            and 0
            <= (datetime.now(timezone.utc) - utc_time(latest_read)).total_seconds()
            <= args.max_age
        )
        if stable and current:
            external, failures = apply_owned_depth(
                books,
                after,
                at=datetime.now(timezone.utc),
                blocked_exchange_ids=context["blocked_exchange_ids"],
            )
            books.update(external)
            for book in external.values():
                book["own_order_evidence"] = {
                    "orders_sha256": content_hash(after),
                    "receipt_sha256s": [
                        r["response_sha256"]
                        for r in reads.requests
                        if r["capability"] == "open_orders"
                    ],
                    "oms_order_check": "READ_ONLY_COMMON_ACCOUNT_VIEW",
                }
        else:
            external, failures = (
                {},
                {eid: "SEMANTIC_OPEN_ORDERS_CHANGED_OR_STALE" for eid in books},
            )
        manifest["account_order_view"] = {
            **context,
            "official_order_lists_equal": stable,
            "official_order_view_current": current,
            "official_open_orders": len(after),
            "external_books_normalized": len(external),
            "book_failures": failures,
            "receipt_manifest": reads.requests,
            "atomic_account_book_snapshot": False,
            "new_orders": 0,
            "new_cancels": 0,
        }
        manifest["own_order_exclusion"] = "PER_BOOK_SEE_ACCOUNT_ORDER_VIEW"
        read_report.update(status="READ_COMPLETE", **manifest["account_order_view"])
        return books, quality, manifest
    finally:
        read_report["receipt_manifest"] = reads.requests
        reads.reader.close()
        reads.key = ""
        if store is not None:
            store.engine.dispose()
        write_json(Path(args.output) / "account_order_read.json", read_report)
