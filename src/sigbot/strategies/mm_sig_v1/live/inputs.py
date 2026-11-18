from datetime import datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from sigbot.domain import ZERO, content_hash
from sigbot.errors import SchemaMismatch
from sigbot.strategies.mm_sig_v1.core import external_best


def review_market(market, nodes, tournament_id):
    contexts = [
        c
        for c in market["contexts"]
        if c["type"] == "tournament" and c["tournament"]["id"] == tournament_id
    ]
    root = nodes["root"]
    if (
        nodes["market_id"] != market["id"]
        or len(contexts) != 1
        or contexts[0]["status"] != "open"
        or market["status"] != "open"
    ):
        raise SchemaMismatch("MM_MARKET_SCOPE_OR_STATUS_INVALID")
    if (
        market["isComposite"]
        or market["isMultiOutcome"]
        or len(market["exchanges"]) != 1
        or root["node_type"] != "contract"
    ):
        raise SchemaMismatch("MM_SINGLE_BINARY_CONTRACT_REQUIRED")
    exchange = market["exchanges"][0]
    if [e["id"] for e in contexts[0]["exchanges"]] != [exchange["id"]]:
        raise SchemaMismatch("MM_CONTEXT_EXCHANGE_MAPPING_CHANGED")
    details = root["contract_details"]
    required = [
        "raceId",
        "stageId",
        "usState",
        "raceName",
        "raceStage",
        "winnerName",
        "electionDate",
        "resolutionType",
    ]
    if root["contract_type"] == "Freeform":
        if (
            not details
            or not isinstance(details.get("description"), str)
            or not details["description"].strip()
        ):
            raise SchemaMismatch("MM_OFFICIAL_FREEFORM_RULES_INCOMPLETE")
        # The official literal settlement rule is the authority. Do not infer
        # cross-market equivalence or a shared event from titles/free text.
        event_binding = ["OFFICIAL_FREEFORM_CONTRACT", market["id"], root["contract_id"]]
        rule_basis = "OFFICIAL_LITERAL_FREEFORM_SETTLEMENT_DESCRIPTION"
    elif root["contract_type"] == "Election Outcome":
        if not details or any(not details.get(k) for k in required):
            raise SchemaMismatch("MM_OFFICIAL_ELECTION_RULES_INCOMPLETE")
        if details["resolutionType"] != "Party Winner" or details["raceStage"] != "General":
            raise SchemaMismatch("MM_RULE_TYPE_NOT_SUPPORTED")
        event_binding = [details["raceId"], details["stageId"], details["electionDate"]]
        rule_basis = "OFFICIAL_STRUCTURED_ELECTION_PREDICATE"
    else:
        raise SchemaMismatch("MM_OFFICIAL_CONTRACT_RULES_NOT_VERIFIED")
    if root["settled_with"] is not None:
        raise SchemaMismatch("MM_RULE_TYPE_NOT_SUPPORTED")
    return {
        "status": "VERIFIED_OFFICIAL_BINARY_SCOPE_AND_PREDICATE",
        "market_id": market["id"],
        "exchange_id": exchange["id"],
        "title": market["title"],
        "event_key": "EVENT-" + content_hash(event_binding)[:24],
        "instrument_id": "SIG-" + tournament_id + "-" + exchange["id"] + "-YES",
        "rules_hash": content_hash(nodes),
        "mapping_hash": content_hash(
            [market["id"], exchange["id"], root["contract_id"], tournament_id]
        ),
        "contract_id": root["contract_id"],
        "contract_details": details,
        "rule_review_basis": rule_basis,
        "settlement_date": root["settlement_date"],
        "nodes": nodes,
        "mapping_basis": "OFFICIAL_SINGLE_BINARY_EXCHANGE_AND_SINGLE_CONTRACT_LEAF; NO_TITLE_PAIRING",
    }


def actual_prices(
    book, inventory: Decimal, tick: Decimal, settings, *, fee=None, capital=None, policy_inputs=None
):
    now = datetime.now(book.received_at.tzinfo)
    if (
        book.quality != "HEALTHY"
        or not book.source_at <= book.received_at <= now
        or book.validated_at is not None
        and book.validated_at > now
        or (
            now
            - (
                book.validated_at
                if (book.continuity_verified or settings.strategy_version == "1.3")
                and book.validated_at is not None
                else book.source_at
            )
        ).total_seconds()
        > settings.book_ttl_seconds
    ):
        raise SchemaMismatch("MM_REAL_BOOK_NOT_CURRENT")
    if settings.strategy_version == "1.3":
        from sigbot.strategies.mm_sig_v1.live.policy import quote

        if policy_inputs is None:
            raise SchemaMismatch("MM13_PERSISTED_POLICY_INPUTS_REQUIRED")
        return quote(book, tick, settings, policy_inputs)
    bid, ask = external_best(book)
    if settings.strategy_version == "1.2":
        if fee is None or capital is None or capital <= 0:
            raise SchemaMismatch("MM_COMPETITIVE_FEE_AND_CAPITAL_REQUIRED")
        buy, sell = bid, ask
        units = settings.quote_quantity or max(
            Decimal(1),
            (capital * settings.quote_capital_fraction / ((bid + ask) / 2)).to_integral_value(
                rounding=ROUND_FLOOR
            ),
        )
        # Improve one tick only while keeping a strictly positive fee-adjusted
        # spread. Inventory determines which side gets price priority.
        if sell - buy - tick - 2 * fee > 0:
            if inventory > units:
                sell -= tick
            else:
                buy += tick
        if not ZERO < buy < sell < 1 or sell - buy - 2 * fee <= 0:
            raise SchemaMismatch("MM_NO_POSITIVE_FEE_ADJUSTED_SPREAD")
        return {
            "bid": str(buy),
            "ask": str(sell),
            "external_bid": str(bid),
            "external_ask": str(ask),
            "quote_spread": str(sell - buy),
            "inventory_quantity": str(inventory),
            "buy_quantity": str(units),
            "sell_quantity": str(min(units, inventory)),
            "minimum_target_net_spread": None,
            "maximum_fee_per_share": str((sell - buy) / 2),
            "net_spread": str(sell - buy - 2 * fee),
            "price_source_at": book.source_at.isoformat(),
            "received_at": book.received_at.isoformat(),
            "sequence": book.sequence,
            "passive_before_submit": True,
            "pricing_policy": "COMPETITIVE_CURRENT_BOOK_POSITIVE_FEE_ADJUSTED_SPREAD",
        }
    assert settings.target_net_spread is not None and settings.max_inventory is not None
    assert settings.quote_quantity is not None
    required = settings.target_net_spread + settings.exit_buffer
    if fee is not None:
        required += 2 * fee
    half = max((ask - bid) / 2, required / 2)
    center = (bid + ask) / 2 - tick * settings.inventory_skew_ticks * min(
        inventory / settings.max_inventory, Decimal(1)
    )
    buy = ((center - half) / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
    sell = ((center + half) / tick).to_integral_value(rounding=ROUND_CEILING) * tick
    buy, sell = min(buy, ask - tick), max(sell, bid + tick)
    if not ZERO < buy < sell < 1 or sell - buy < required:
        raise SchemaMismatch("MM_PASSIVE_MARGIN_NOT_AVAILABLE_WITHIN_PRICE_BOUNDS")
    return {
        "bid": str(buy),
        "ask": str(sell),
        "external_bid": str(bid),
        "external_ask": str(ask),
        "quote_spread": str(sell - buy),
        "inventory_quantity": str(inventory),
        "buy_quantity": str(
            min(settings.quote_quantity, max(ZERO, settings.max_inventory - inventory))
        ),
        "sell_quantity": str(min(settings.quote_quantity, inventory)),
        "minimum_target_net_spread": str(settings.target_net_spread),
        "maximum_fee_per_share": str(
            (sell - buy - settings.target_net_spread - settings.exit_buffer) / 2
        ),
        "net_spread": str(sell - buy - 2 * fee - settings.exit_buffer) if fee is not None else None,
        "price_source_at": book.source_at.isoformat(),
        "received_at": book.received_at.isoformat(),
        "sequence": book.sequence,
        "passive_before_submit": True,
    }


def actual_inventory(data, exchange_id):
    rows = [p for p in data["positions"]["positions"] if p["exchangeId"] == exchange_id]
    if len(rows) > 1:
        raise SchemaMismatch("MM_DUPLICATE_ACCOUNT_POSITION")
    if rows and rows[0]["quantity"] < 0:
        raise SchemaMismatch("MM_NO_HOLDING_NOT_SUPPORTED_FOR_YES_QUOTES")
    return Decimal(str(rows[0]["quantity"])) if rows else ZERO
