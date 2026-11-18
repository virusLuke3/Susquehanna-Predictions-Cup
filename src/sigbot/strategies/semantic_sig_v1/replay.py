"""Receipt-bound S3 depth/cost scenarios. Missing inputs remain NOT_EVALUATED."""

from datetime import timedelta
from decimal import Decimal

from sigbot.domain import utc_time
from sigbot.errors import SchemaMismatch
from sigbot.strategies.semantic_sig_v1.core import number


def consume(levels, requested, *, buy):
    parsed = [(number(x["price"], probability=True), number(x["quantity"])) for x in levels]
    if parsed != sorted(parsed, reverse=not buy) or len({p for p, q in parsed}) != len(parsed):
        raise SchemaMismatch("SEMANTIC_REPLAY_BOOK_SORT_OR_DUPLICATE_INVALID")
    remaining, value = requested, Decimal(0)
    for item, (price, quantity) in zip(levels, parsed, strict=True):
        if item.get("own") is not False:
            raise SchemaMismatch("SEMANTIC_REPLAY_OWN_DEPTH_UNKNOWN")
        taken = min(remaining, quantity)
        value += taken * price
        remaining -= taken
    return requested - remaining, value


def depth_scenario(row, *, synthetic=False):
    """Capacity is conditional on supplied audited books, never a guaranteed fill."""
    result: dict = {"status": "NOT_EVALUATED", "real_pnl": None, "reasons": []}
    try:
        quantity = number(row["quantity"], positive=True)
        if quantity != quantity.to_integral_value():
            raise SchemaMismatch("SEMANTIC_REPLAY_INTEGER_QUANTITY_REQUIRED")
        created, entry_at, exit_at = (
            utc_time(row[k]) for k in ["created_at", "entry_at", "exit_at"]
        )
        if not created <= entry_at < exit_at <= created + timedelta(seconds=300):
            raise SchemaMismatch("SEMANTIC_REPLAY_WINDOW_OR_LATENCY_INVALID")
        fee = row.get("fee_contract")
        if not fee or fee.get("verified") is not True or not fee.get("evidence_sha256"):
            raise SchemaMismatch("SEMANTIC_REPLAY_FEE_NOT_VERIFIED")
        per_share = number(fee["per_share"])
        for key, at in [("entry_book", entry_at), ("exit_book", exit_at)]:
            book = row[key]
            if (
                book.get("data_provenance") != ("SYNTHETIC" if synthetic else "OFFICIAL_REAL")
                or book.get("quality") != "HEALTHY"
                or book.get("own_orders_known") is not True
                or not book.get("raw_sha256")
                or book.get("instrument_id") != row["instrument_id"]
                or book.get("rules_hash") != row["rules_hash"]
                or not 0 <= (at - utc_time(book["received_at"])).total_seconds() <= 5
                or utc_time(book["rules_available_at"]) > created
            ):
                raise SchemaMismatch("SEMANTIC_REPLAY_PIT_IDENTITY_RULES_OR_EXTERNAL_DEPTH_INVALID")
        if synthetic != row["instrument_id"].startswith("SIM-"):
            raise SchemaMismatch("SEMANTIC_REPLAY_NAMESPACE_MISMATCH")
        bought, cost = consume(row["entry_book"]["asks"], quantity, buy=True)
        if bought <= 0:
            raise SchemaMismatch("SEMANTIC_REPLAY_NO_ENTRY_CAPACITY")
        sold, proceeds = consume(row["exit_book"]["bids"], bought, buy=False)
        # FIFO entry costs allocate only to the modeled quantity actually exited.
        matched_cost = Decimal(0)
        remaining = sold
        for level in row["entry_book"]["asks"]:
            taken = min(remaining, number(level["quantity"]))
            matched_cost += taken * number(level["price"])
            remaining -= taken
        result.update(
            status="COMPLETE_COST_SCENARIO" if sold == quantity else "PARTIAL_WITH_RESIDUAL_RISK",
            requested_quantity=str(quantity),
            modeled_entry_capacity=str(bought),
            modeled_exit_capacity=str(sold),
            residual_inventory=str(bought - sold),
            unfilled_entry_quantity=str(quantity - bought),
            entry_cost=str(cost),
            exit_proceeds=str(proceeds),
            fees=str((bought + sold) * per_share),
            matched_cost_pnl=str(proceeds - matched_cost - 2 * sold * per_share),
            residual_cost=str(cost - matched_cost + (bought - sold) * per_share),
            synthetic_data_used=synthetic,
            queue_fill_probability=None,
            scope="CONDITIONAL_DEPTH_COST_SCENARIO_NOT_OBSERVED_FILLS",
        )
    except (SchemaMismatch, KeyError, ValueError, TypeError) as exc:
        result["reasons"] = [
            exc.code
            if isinstance(exc, SchemaMismatch)
            else "SEMANTIC_REPLAY_INPUT_MISSING_OR_INVALID"
        ]
    return result
