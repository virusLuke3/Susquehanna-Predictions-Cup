"""Official account observations and independent, exact financial reconstruction.

This never submits orders or repairs a trading ledger from snapshots. Complete
history is read twice; an unchanged financial state is a bracketed checkpoint,
not an atomic exchange snapshot. Market valuations are intentionally excluded
from the financial stability signature.
"""

from __future__ import annotations

import time
from collections import defaultdict
from copy import deepcopy
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from sigbot.domain import content_hash
from sigbot.errors import SchemaMismatch

ZERO = Decimal(0)
RECONSTRUCTION_VERSION = "FIFO_AND_MOVING_AVERAGE_BINARY_NETTING_V5"


def amount(value) -> Decimal:
    if isinstance(value, (float, bool)):
        raise SchemaMismatch("ACCOUNT_DECIMAL_INPUT_REQUIRED")
    value = Decimal(str(value))
    if not value.is_finite():
        raise SchemaMismatch("ACCOUNT_NONFINITE_AMOUNT")
    return value


def financial_state(data: dict) -> dict:
    positions = [
        {
            **{
                k: p[k]
                for k in ("exchangeId", "marketId", "quantity", "costBasis", "settled", "lots")
            },
            **({"avgCost": p["avgCost"]} if "avgCost" in p else {}),
        }
        for p in data["positions"]["positions"]
    ]
    transactions = [
        {k: v for k, v in t.items() if k not in {"currentPrice", "marketTitle", "marketImage"}}
        for t in data["transactions"]
    ]
    return {
        "cash": data["cup"]["myBalance"],
        "initial": data["cup"]["initialBalance"],
        "positions": sorted(positions, key=lambda p: p["exchangeId"]),
        "orders": sorted(data["orders"], key=lambda o: o["id"]),
        "fills": sorted(data["fills"], key=lambda f: (f["filledAt"], f["orderId"] or -1, f["id"])),
        "transactions": sorted(transactions, key=lambda t: t["event_id"]),
        "settlements": sorted(data["settlements"], key=lambda s: content_hash(s)),
    }


def read_round(reads) -> dict:
    scope = {"slug": reads.slug}
    cup = reads.call("tournament_identity", path_params=scope)
    if cup["id"] != reads.reader.config.expected_tournament_id or cup["isPendingEnrolment"]:
        raise SchemaMismatch("ACCOUNT_CHECKPOINT_SCOPE_MISMATCH")
    contract = getattr(getattr(reads, "reader", None), "contract", None)
    archive_policy = getattr(contract, "data", {}).get("order_history_policy") == (
        "CANCELLED_UNFILLED_ARCHIVED_AFTER_48H"
    )
    orders, coverage = reads.check(
        "open_orders",
        lambda: reads.pages(
            "open_orders",
            query={"tournamentId": cup["id"], "status": "all", "limit": 200},
            **({"allow_archived_orders": True} if archive_policy else {}),
        ),
    )
    order_coverage = (
        getattr(reads, "history_coverage", {}).get("open_orders", [])
        or getattr(reads, "checks", {}).get("open_orders", {}).get("coverage", [])
        or (coverage if isinstance(coverage, list) else [])
    )
    history_complete = (
        not any(c["complete"] is False for c in order_coverage)
        if order_coverage and all(isinstance(c, dict) for c in order_coverage)
        else None
    )
    if archive_policy and history_complete is None:
        raise SchemaMismatch("ORDER_HISTORY_COVERAGE_REQUIRED")
    if history_complete is False:
        # Archived history is not current-open-order proof. Read that projection
        # independently and require complete coverage and the same open identities.
        current, _ = reads.pages(
            "open_orders", query={"tournamentId": cup["id"], "status": "open", "limit": 200}
        )
        if sorted((o["id"], content_hash(o)) for o in current) != sorted(
            (o["id"], content_hash(o)) for o in orders if o["open"]
        ) or any(not o["open"] for o in current):
            raise SchemaMismatch("CURRENT_OPEN_ORDER_PROJECTION_CHANGED")
    positions = reads.call("positions", path_params=scope)
    fills, _ = reads.check(
        "fills", lambda: reads.pages("fills", path_params=scope, query={"limit": 200})
    )
    transactions, _ = reads.check(
        "transactions", lambda: reads.pages("transactions", path_params=scope, query={"limit": 200})
    )
    settlements, _ = reads.check(
        "settlement_history",
        lambda: reads.pages("settlement_history", path_params=scope, query={"limit": 200}),
    )
    data = {
        "cup": cup,
        "orders": orders,
        "positions": positions,
        "fills": fills,
        "transactions": transactions,
        "settlements": settlements,
        "pagination_complete": True,
        "order_history_complete": history_complete,
        "order_history_coverage": order_coverage,
    }
    # All current financial endpoints remain fresh. Reuse only exact NO
    # order/fill identities from a PASS proof, with a bounded canonical audit.
    cache_scope = [getattr(reads.reader.config, "expected_account_id", None), cup["id"]]
    cached = getattr(reads, "_verified_no_fill_cache", None) or {}
    previous = cached.get("orders", {}) if cached.get("scope") == cache_scope else {}
    bracket = getattr(reads, "_bracket_no_fill_cache", None)
    if bracket is not None and bracket.get("scope") != cache_scope:
        bracket.clear()
        bracket.update(scope=cache_scope, orders={})
    evidence: dict = {"scope": cache_scope, "orders": {}}
    stamp = time.monotonic()
    fetched, reused, reused_verified = 0, 0, 0
    orders_by_id = {o["id"]: o for o in orders}
    no_ids = {f["orderId"] for f in fills if amount(f["quantity"]) < 0}
    for oid in sorted(no_ids):
        group = [f for f in fills if f["orderId"] == oid]
        order = orders_by_id.get(oid, {})
        binding = content_hash([cache_scope, group, {k: order.get(k) for k in
                               ("id", "side", "action", "exchangeId", "tournamentId")}])
        in_bracket = bracket["orders"].get(str(oid)) if bracket is not None else None
        prior = in_bracket or previous.get(str(oid))
        if prior and prior["raw_digest"] == binding and 0 <= stamp - prior["verified_at"] < 300:
            exact = prior["fills"]
            entry = prior
            reused += 1
            reused_verified += int(in_bracket is None)
        else:
            exact, _ = reads.pages(
                "order_fills", path_params={"id": str(oid)}, query={"limit": 200}
            )
            entry = {"raw_digest": binding, "fills": deepcopy(exact), "verified_at": stamp}
            fetched += 1
            if bracket is not None:
                bracket["orders"][str(oid)] = entry
        indexed = {f["id"]: f for f in exact}
        for f in group:
            canonical = indexed.get(f["id"])
            if (canonical is None or canonical["side"] != f["side"]
                    or amount(canonical["quantity"]) != amount(f["quantity"])):
                raise SchemaMismatch("ACCOUNT_NO_FILL_IDENTITY_OR_QUANTITY_MISMATCH")
            f["price"] = canonical["price"]
        evidence["orders"][str(oid)] = entry
    data["_no_fill_evidence"] = evidence
    data["_read_metrics"] = {
        "canonical_no_order_reads": fetched,
        "canonical_no_orders_reused": reused,
        "canonical_no_verified_orders_reused": reused_verified,
    }
    return data


def display_cent_compatible(exact: Decimal, reported: Decimal) -> bool:
    """Nearest cent, allowing either tie direction without selecting an unknown mode."""
    return display_compatible(exact, reported, Decimal("0.01"))


def display_compatible(exact: Decimal, reported: Decimal, quantum: Decimal) -> bool:
    if reported != reported.quantize(quantum):
        return False
    low = exact.quantize(quantum, rounding=ROUND_FLOOR)
    high = exact.quantize(quantum, rounding=ROUND_CEILING)
    distance = min(abs(exact - low), abs(high - exact))
    return reported in {low, high} and abs(reported - exact) == distance


def reconstruct(data: dict, *, cash_display_places: int | None = None) -> dict:
    issues: list[str] = []
    journal: list[dict] = []
    orders = {o["id"]: o for o in data["orders"]}
    if len(orders) != len(data["orders"]):
        issues.append("DUPLICATE_ORDER")
    tid = data["cup"]["id"]
    if any(o["tournamentId"] != tid for o in orders.values()):
        issues.append("ORDER_SCOPE_MISMATCH")
    cash, non_trade_cash = ZERO, ZERO
    quantities: dict = defaultdict(lambda: ZERO)
    costs: dict = defaultdict(lambda: ZERO)
    average_costs: dict = defaultdict(lambda: ZERO)
    lots: dict = defaultdict(list)
    transaction_ids: set = set()
    unsupported = []
    opening = []
    for tx in data["transactions"]:
        key = tx["event_id"]
        if key in transaction_ids:
            issues.append("DUPLICATE_TRANSACTION")
            continue
        transaction_ids.add(key)
        if tx["tournamentId"] != tid:
            issues.append("TRANSACTION_SCOPE_MISMATCH")
        if tx["event_type"] == "trade":
            continue  # Fills are authoritative; never book this same trade twice.
        if tx["event_type"] == "deposit" and tx.get("transactionType") == "OPENING_BALANCE":
            opening.append(tx)
        if tx["event_type"] not in {"deposit", "fee"} or tx.get("transactionType") not in {
            None,
            "OPENING_BALANCE",
        }:
            unsupported.append(key)
            continue
        if tx["amount"] is None:
            issues.append("NON_TRADE_CASH_AMOUNT_MISSING")
            continue
        delta = amount(tx["amount"])
        cash += delta
        non_trade_cash += delta
        journal.append(
            {"id": "transaction:" + key, "cash_delta": str(delta), "kind": tx["event_type"]}
        )
    if len(opening) != 1 or amount(opening[0]["amount"]) != amount(data["cup"]["initialBalance"]):
        issues.append("OPENING_BALANCE_NOT_VERIFIED")
    if unsupported or data["settlements"]:
        issues.append("UNSUPPORTED_SETTLEMENT_OR_COLLATERAL_HISTORY")
    fill_keys: set = set()
    # The order ID and time also disambiguate the documented legacy/engine trade-ID spaces.
    for fill in sorted(data["fills"], key=lambda f: (f["filledAt"], f["id"])):
        key = content_hash([tid, fill["orderId"], fill["id"], fill["filledAt"], fill["side"]])
        if key in fill_keys:
            issues.append("DUPLICATE_FILL")
            continue
        fill_keys.add(key)
        order = orders.get(fill["orderId"])
        if (
            order is None
            or order["exchangeId"] != fill["exchangeId"]
            or order["side"] != fill["side"]
        ):
            issues.append("FILL_ORDER_LINK_UNVERIFIED")
            continue
        signed = amount(fill["quantity"])
        if not signed or (signed > 0) != (fill["side"] == "yes") or fill["price"] is None:
            issues.append("FILL_OUTCOME_OR_PRICE_UNVERIFIED")
            continue
        q, price = abs(signed), amount(fill["price"])
        if not ZERO < price < 1:
            issues.append("FILL_PRICE_OUT_OF_BOUNDS")
            continue
        outcome = "YES" if signed > 0 else "NO"
        instrument = fill["exchangeId"] + ":" + outcome
        other = fill["exchangeId"] + (":NO" if outcome == "YES" else ":YES")
        if order["action"] == "buy":
            delta = -q * price
            quantities[instrument] += q
            costs[instrument] += q * price
            average_costs[instrument] += q * price
            lots[instrument].append([q, q * price])
            paired = min(quantities[instrument], quantities[other])
            if paired:
                for side in (instrument, other):
                    average_costs[side] *= (quantities[side] - paired) / quantities[side]
                    left, removed = paired, ZERO
                    for lot in lots[side]:
                        take = min(left, lot[0])
                        if not take:
                            continue
                        consumed = lot[1] * take / lot[0]
                        lot[0] -= take
                        lot[1] -= consumed
                        left -= take
                        removed += consumed
                    quantities[side] -= paired
                    costs[side] -= removed
                # REST fills use the outcome price; each opposite pair pays 1.
                delta += paired
        elif order["action"] == "sell" and quantities[instrument] >= q:
            delta, left, removed = q * price, q, ZERO
            average_costs[instrument] *= (quantities[instrument] - q) / quantities[instrument]
            for lot in lots[instrument]:
                take = min(left, lot[0])
                if not take:
                    continue
                cost = lot[1] * take / lot[0]
                lot[0] -= take
                lot[1] -= cost
                left -= take
                removed += cost
            quantities[instrument] -= q
            costs[instrument] -= removed
        else:
            issues.append("UNBACKED_OR_UNCLASSIFIED_SELL")
            continue
        cash += delta
        journal.append(
            {
                "id": "fill:" + key,
                "cash_delta": str(delta),
                "instrument": instrument,
                "quantity_delta": str(q if order["action"] == "buy" else -q),
                "kind": "FILL_NOMINAL_CASH",
            }
        )
    projected = {k: {"quantity": str(q), "cost": str(costs[k])} for k, q in quantities.items() if q}
    remote = {}
    display_differences = []
    for pos in data["positions"]["positions"]:
        signed, cost = amount(pos["quantity"]), amount(pos["costBasis"])
        if not signed:
            continue
        side = "YES" if signed > 0 else "NO"
        key = pos["exchangeId"] + ":" + side
        if key in remote:
            issues.append("DUPLICATE_POSITION")
        actual_lots = pos["lots"]
        if any(lot["side"].upper() != side for lot in actual_lots):
            issues.append("LOT_SIDE_MISMATCH")
        if sum((amount(lot["quantity"]) for lot in actual_lots), ZERO) != abs(signed):
            issues.append("LOT_QUANTITY_MISMATCH")
        lot_cost = sum(
            (amount(lot["quantity"]) * amount(lot["entryPrice"]) for lot in actual_lots), ZERO
        )
        # The execution contract identifies FIFO lots as the cost evidence. Keep
        # the rounded summary separate; never replace exact lot cost with it.
        remote[key] = {"quantity": str(abs(signed)), "cost": str(lot_cost)}
        rounded_fifo_summary = cash_display_places == 2 and display_cent_compatible(lot_cost, cost)
        # The API exposes both a display average and FIFO lots. A partial sale
        # can leave the display average unchanged while consuming the oldest
        # lots. Verify that separate summary against an independent moving-
        # average reconstruction; the ledger still requires exact FIFO costs.
        average_summary = (
            key in quantities
            and quantities[key] == abs(signed)
            and lot_cost == costs[key]
            and "avgCost" in pos
            and display_compatible(
                average_costs[key] / abs(signed), amount(pos["avgCost"]), Decimal("0.000001")
            )
            and (
                cost == average_costs[key]
                or cash_display_places == 2
                and display_cent_compatible(average_costs[key], cost)
            )
        )
        if lot_cost != cost and not (rounded_fifo_summary or average_summary):
            issues.append("LOT_COST_MISMATCH")
        elif lot_cost != cost:
            display_differences.append(
                {
                    "field": "costBasis",
                    "instrument": key,
                    "exact_fifo_cost": str(lot_cost),
                    "reported_summary": str(cost),
                    "basis": "INDEPENDENT_MOVING_AVERAGE_SUMMARY; EXACT_FIFO_LOTS_MATCH_FILLS"
                    if average_summary and not rounded_fifo_summary
                    else "INSTANCE_NEAREST_CENT; EXACT_LOTS_INDEPENDENTLY_MATCH_FILLS",
                    "reconstructed_average_cost": str(average_costs[key]),
                }
            )

    # Decimal equality, not textual equality (1 == 1.0).
    def numeric(ps):
        return {k: (amount(v["quantity"]), amount(v["cost"])) for k, v in ps.items()}

    position_differences = {
        key: {
            "fill_cost": str(costs.get(key, ZERO)),
            "reported_cost": str(remote.get(key, {}).get("cost")),
            "cost_delta": str(amount(remote[key]["cost"]) - costs.get(key, ZERO))
            if key in remote
            else None,
        }
        for key in set(projected) | set(remote)
        if numeric(projected).get(key) != numeric(remote).get(key)
    }
    if position_differences:
        issues.append("POSITION_OR_COST_MISMATCH")
    unexplained = amount(data["cup"]["myBalance"]) - cash
    rounded_cash = cash_display_places == 2 and display_cent_compatible(
        cash, amount(data["cup"]["myBalance"])
    )
    if unexplained and not rounded_cash:
        issues.append("UNEXPLAINED_CASH_CHANGE")
    elif unexplained:
        display_differences.append(
            {
                "field": "myBalance",
                "exact_history_cash": str(cash),
                "reported_summary": str(data["cup"]["myBalance"]),
                "basis": "OFFICIAL_MYBALANCE_ROUNDED_TO_2_DECIMALS",
            }
        )
    tx_quantity: dict = defaultdict(lambda: ZERO)
    fill_quantity: dict = defaultdict(lambda: ZERO)
    for tx in data["transactions"]:
        if tx["event_type"] == "trade":
            tx_quantity[
                (
                    tx["exchangeId"],
                    "no" if amount(tx["quantity"]) < 0 else (tx["settlementOption"] or "").lower(),
                    (tx["orderType"] or "").lower(),
                )
            ] += abs(amount(tx["quantity"]))
    for f in data["fills"]:
        if f["orderId"] in orders:
            fill_quantity[(f["exchangeId"], f["side"], orders[f["orderId"]]["action"])] += abs(
                amount(f["quantity"])
            )
    if dict(tx_quantity) != dict(fill_quantity):
        issues.append("TRANSACTION_FILL_QUANTITY_MISMATCH")
    # Transaction price may be a rounded aggregate; do not invent a per-fill ID
    # linkage or book rounded price*quantity instead of the actual fills.
    return {
        "reconstruction_version": RECONSTRUCTION_VERSION,
        "status": "PASS" if not issues else "FAIL",
        "differences": sorted(set(issues)),
        "projected_cash": str(cash),
        "remote_cash": str(data["cup"]["myBalance"]),
        "unexplained_cash": str(unexplained),
        "unexplained_economic_cash": "0" if rounded_cash else str(unexplained),
        "display_differences": display_differences,
        "positions": projected,
        "position_differences": position_differences,
        "reporting_precision": "DOCUMENTED_MYBALANCE_2DP; FIFO_COST_EXACT; DISPLAY_AVERAGE_INDEPENDENT"
        if cash_display_places == 2
        else "NOT_DOCUMENTED_NO_TOLERANCE_ASSUMED",
        "journal": journal,
        "journal_cash_total": str(sum((amount(j["cash_delta"]) for j in journal), ZERO)),
        "history_counts": {
            k: len(data[k]) for k in ("orders", "fills", "transactions", "settlements")
        },
        "open_orders": sum(o["open"] for o in orders.values()),
        "transaction_linkage": "INDEPENDENT_QUANTITY_CROSS_CHECK_NOT_PER_FILL_ID_LINKAGE",
        "fees": "ACTUAL_FEE_ROWS_AND_CASH_RESIDUAL_CHECKED_NOT_FUTURE_FEE_SCHEDULE",
        "production_oms_ledger_acceptance": False,
    }


def checkpoint(reads, *, previous=None) -> tuple[dict, dict]:
    # Fresh, schema-validated canonical receipts may serve both halves of this
    # same bracket. They become a reusable proof only after full PASS, and are
    # discarded on failure; no second network fetch of identical history.
    if reads is None:
        return _checkpoint(reads, previous=previous)
    old = getattr(reads, "_bracket_no_fill_cache", None)
    reads._bracket_no_fill_cache = {}
    try:
        return _checkpoint(reads, previous=previous)
    finally:
        if old is None:
            del reads._bracket_no_fill_cache
        else:
            reads._bracket_no_fill_cache = old


def _checkpoint(reads, *, previous=None) -> tuple[dict, dict]:
    rolling = previous is not None
    first, second = (previous if rolling else read_round(reads)), read_round(reads)
    before, after = content_hash(financial_state(first)), content_hash(financial_state(second))
    if rolling and before != after:
        # Every mutation/correction requires a fresh second complete round.
        # Never use a previous cash/position summary to bless new history.
        first, second = second, read_round(reads)
        before, after = content_hash(financial_state(first)), content_hash(financial_state(second))
        rolling = False
    reader = getattr(reads, "reader", None)
    description = (
        reader.schema.spec["paths"]["/tournaments/{slug}"]["get"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]["properties"]["myBalance"].get("description", "")
        if reader is not None
        else ""
    )
    places = 2 if "rounded to 2 decimals" in description else None
    result = reconstruct(second, cash_display_places=places)
    if (reads is not None and result["status"] != "PASS"
            and second.get("_read_metrics", {}).get("canonical_no_verified_orders_reused", 0)):
        # A current balance/lot/history contradiction cannot be masked by cached
        # prices. Retry once from two full, uncached rounds; preserve real failure.
        reads._verified_no_fill_cache = None
        return checkpoint(reads)
    result["precision_evidence"] = {
        "schema_path": reader.schema.path if reader is not None else None,
        "myBalance_description": description,
    }
    if before != after:
        result["status"] = "INDETERMINATE"
        result["differences"].append("ACCOUNT_CHANGED_DURING_BRACKET")
    result.update(
        order_history_complete=second.get("order_history_complete", True),
        checkpoint=content_hash(
            [
                before,
                after,
                RECONSTRUCTION_VERSION,
                result["precision_evidence"],
                "ROLLING_COMPLETE_V1",
            ]
            if rolling
            else [before, after, RECONSTRUCTION_VERSION, result["precision_evidence"]]
        ),
        financial_state_digest=after,
        financial_state_unchanged=before == after,
        consistency="TWO_COMPLETE_READS_BRACKETED_FINANCIAL_STATE_NOT_ATOMIC",
        pagination_complete=True,
        real_orders=0,
        real_cancels=0,
    )
    if rolling:
        result["read_bracket_policy"] = "PREVIOUS_VERIFIED_COMPLETE_PLUS_CURRENT_COMPLETE"
    if reads is not None and result["status"] == "PASS" and "_no_fill_evidence" in second:
        reads._verified_no_fill_cache = second["_no_fill_evidence"]
    # Coverage sequences are per-read provenance, retained in data/raw receipts.
    # They must not change an immutable financial checkpoint's payload. Version
    # the proof identity and bind completeness so older proofs remain untouched.
    result["checkpoint"] = content_hash(
        [result["checkpoint"], "ORDER_HISTORY_ARCHIVE_V1", result["order_history_complete"]]
    )
    return result, second


def persist_checkpoint(store, account_id: str, result: dict, data: dict, observed_at) -> None:
    from sigbot.errors import AuthorizationRequired, Conflict
    from sigbot.storage import AccountReadCheckpoint

    if store.namespace not in {"offline", "readonly"} or (
        (store.namespace == "offline") != account_id.startswith("SIM-")
    ):
        raise AuthorizationRequired("ACCOUNT_READ_NAMESPACE_MISMATCH")
    payload = {"result": result, "financial_state": financial_state(data)}
    # Normalize Decimal strings once for both PostgreSQL JSON and digest equality.
    import json

    payload = json.loads(json.dumps(payload, default=str))
    digest = content_hash(payload)
    key = (store.namespace, account_id, data["cup"]["id"], result["checkpoint"])
    with store.transaction() as session:
        old = session.get(AccountReadCheckpoint, key)
        if old:
            if old.digest != digest:
                raise Conflict("READ_CHECKPOINT_CONTENT_CONFLICT")
            return
        session.add(
            AccountReadCheckpoint(
                namespace=store.namespace,
                account_id=account_id,
                tournament_id=data["cup"]["id"],
                checkpoint=result["checkpoint"],
                status=result["status"],
                observed_at=observed_at,
                data=payload,
                digest=digest,
            )
        )
