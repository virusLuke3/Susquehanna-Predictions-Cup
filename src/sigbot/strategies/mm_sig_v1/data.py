"""Finite official GET-only scan. Never starts services or imports trading state."""

import hashlib
import json
import re
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import httpx

from sigbot.domain import ZERO, content_hash, utc_time
from sigbot.errors import (
    AuthorizationRequired,
    ContractNotVerified,
    RiskRejected,
    SchemaMismatch,
    SigbotError,
)
from sigbot.history import tape
from sigbot.official import OfficialSchema
from sigbot.readcheck import ReadCheck, load_key
from sigbot.storage_policy import bounded_storage
from sigbot.strategies.mm_sig_v1.core import external_best, quote_prices
from sigbot.strategies.mm_sig_v1.models import Book, MmConfig
from sigbot.transport import OFFICIAL_ORIGIN


def reviewed_schema():
    contract = json.loads(Path("contracts/strategies/mm_sig_v1.reads.json").read_text())
    path = Path(contract["schema_path"])
    if (
        contract["source_url"] != OFFICIAL_ORIGIN + "/api/v1/docs"
        or contract["writes_enabled"]
        or hashlib.sha256(path.read_bytes()).hexdigest() != contract["schema_sha256"]
    ):
        raise ContractNotVerified("MM_READ_CONTRACT_EVIDENCE_MISMATCH")
    return contract, OfficialSchema(str(path))


def participant_reference(spec, operations):
    """Compare reviewed operations and every transitive schema, including economic text."""
    result = {key: spec.get(key) for key in ("openapi", "servers", "security")}
    # Display title/version are not trading semantics. Economic prose stays
    # review-bound; normalizing whitespace does not approve a new rule.
    result["info"] = {"description": spec.get("info", {}).get("description", "")}
    result["security_schemes"] = spec.get("components", {}).get("securitySchemes", {})
    result["operations"], result["references"] = {}, {}

    def references(value):
        if isinstance(value, list):
            for item in value:
                references(item)
        elif isinstance(value, dict):
            ref = value.get("$ref")
            if ref and ref not in result["references"]:
                if not ref.startswith("#/"):
                    raise ContractNotVerified("MM_EXTERNAL_SCHEMA_REFERENCE_UNVERIFIED")
                resolved = spec
                for part in ref[2:].split("/"):
                    resolved = resolved[part.replace("~1", "/").replace("~0", "~")]
                result["references"][ref] = resolved
                references(resolved)
            for item in value.values():
                references(item)

    try:
        for path, method in operations:
            item = spec["paths"][path]
            operation = {"operation": item[method], "parameters": item.get("parameters", [])}
            result["operations"][method + " " + path] = operation
            references(operation)
    except (KeyError, TypeError) as exc:
        raise ContractNotVerified("MM_REVIEWED_OPERATION_OR_SCHEMA_MISSING") from exc

    def normalize(value):
        if isinstance(value, list):
            return [normalize(item) for item in value]
        if isinstance(value, dict):
            return {
                k: " ".join(v.split())
                if k == "description" and isinstance(v, str)
                else normalize(v)
                for k, v in value.items()
            }
        return value

    return normalize(result)


def current_reference(directory: Path, schema, transport=None):
    """Observe public documentation; keep the verified adapter pinned (AGENTS 18)."""
    import difflib
    from uuid import uuid4

    url = OFFICIAL_ORIGIN + "/api/v1/docs"
    at = datetime.now(timezone.utc)
    evidence = directory / ("current" if bounded_storage() else at.strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid4().hex[:8])
    evidence.mkdir(parents=True, exist_ok=bounded_storage())
    contract = json.loads(Path("contracts/strategies/mm_sig_v1.reads.json").read_text())
    operations = contract.get("reviewed_operations")
    reviewed = participant_reference(schema.spec, operations) if operations else schema.spec
    (evidence / "PINNED_OPENAPI.json").write_text(json.dumps(schema.spec, indent=2) + "\n")
    report = {"policy": "REPORT_ONLY", "url": url, "observed_at": at.isoformat(),
              "reviewed_digest": content_hash(reviewed), "reviewed_operations": operations,
              "matches_reviewed_schema": None, "full_document_matches": None,
              "execution_contract": "PINNED_UNCHANGED", "evidence_directory": str(evidence)}
    response = None
    try:
        with httpx.Client(transport=transport, timeout=20, trust_env=False, follow_redirects=False) as client:
            response = client.get(url)
        (evidence / "PUBLIC_REFERENCE.html").write_bytes(response.content)
        report.update(http_status=response.status_code, response_sha256=hashlib.sha256(response.content).hexdigest())
        response.raise_for_status()
        text = response.text
        start = text.index("Scalar.createApiReference")
        payload, _ = json.JSONDecoder().raw_decode(text[text.index("{", start):])
        spec = payload["content"]
        (evidence / "OBSERVED_OPENAPI.json").write_text(json.dumps(spec, indent=2) + "\n")
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        # No exception text: it may contain credential-bearing URLs or headers.
        report.update(status="REFERENCE_UNAVAILABLE_REPORTED", change_class="UNAVAILABLE")
    else:
        try:
            observed = participant_reference(spec, operations) if operations else spec
        except ContractNotVerified as exc:
            report.update(status="DOCUMENT_CHANGE_REPORTED", change_class="STRUCTURAL", source_code=exc.code,
                          matches_reviewed_schema=False, full_document_matches=False)
        else:
            same = content_hash(observed) == content_hash(reviewed)
            def shape(value):
                if isinstance(value, list):
                    return [shape(v) for v in value]
                if isinstance(value, dict):
                    return {k: shape(v) for k,v in value.items() if k != "description"}
                return value
            report.update(status="MATCHES_PINNED" if same else "DOCUMENT_CHANGE_REPORTED",
                          matches_reviewed_schema=same, full_document_matches=content_hash(spec)==content_hash(schema.spec),
                          observed_digest=content_hash(observed),
                          change_class="NONE" if same else "STRUCTURAL" if shape(observed)!=shape(reviewed) else "SEMANTIC_TEXT",
                          changed_operations=[k for k,v in reviewed.get("operations", {}).items() if observed.get("operations", {}).get(k)!=v])
        diff = list(difflib.unified_diff(json.dumps(schema.spec,indent=2).splitlines(), json.dumps(spec,indent=2).splitlines(), fromfile="pinned", tofile="observed"))
        (evidence / "DOCUMENT.diff").write_text("\n".join(diff) + "\n")
        report["possible_impact"] = "Existing verified operations continue; actual response/schema/identity/ledger errors retain scoped handling."
    (evidence / "REFERENCE_OBSERVATION.json").write_text(json.dumps(report, indent=2) + "\n")
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "REFERENCE_DIFF.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def outcome_book(book: Book, outcome: str) -> Book:
    """Mirror a verified binary book without changing its timestamps or evidence."""
    if outcome == "YES":
        return book
    if outcome != "NO":
        raise SchemaMismatch("MM_BINARY_OUTCOME_REQUIRED")
    return book.model_copy(
        update={
            "bids": sorted(
                [level.model_copy(update={"price": 1 - level.price}) for level in book.asks],
                key=lambda level: level.price,
                reverse=True,
            ),
            "asks": sorted(
                [level.model_copy(update={"price": 1 - level.price}) for level in book.bids],
                key=lambda level: level.price,
            ),
        }
    )


def normalized_book(
    raw: dict, orders: list[dict], received: datetime, *, outcome="YES", previous=None
) -> Book:
    """Documented YES-normalized book; open quantity is the executable remainder."""
    if raw["asOf"] is None:
        raise SchemaMismatch("MM_BOOK_ENGINE_VERSION_UNKNOWN")
    owned: dict[tuple[str, Decimal], Decimal] = {}
    for order in orders:
        if order["exchangeId"] != raw["exchangeId"] or not order["open"]:
            continue
        if order["priceLimit"] is None or order["quantity"] <= 0:
            raise SchemaMismatch("MM_OWN_ORDER_CANNOT_NORMALIZE")
        if order.get("createdAt") and utc_time(order["createdAt"]) > utc_time(raw["asOf"]["at"]):
            raise SchemaMismatch("MM_OWN_ORDER_NEWER_THAN_BOOK")
        if order["expirationDate"] is not None and utc_time(order["expirationDate"]) <= received:
            raise SchemaMismatch("MM_EXPIRED_OPEN_ORDER_REQUIRES_RECONCILIATION")
        p = Decimal(str(order["priceLimit"]))
        # Complement conversion is explicitly documented by the official Guide.
        p = p if order["side"] == "yes" else 1 - p
        bid = (order["side"] == "yes") == (order["action"] == "buy")
        key = ("bids" if bid else "asks", p)
        owned[key] = owned.get(key, ZERO) + Decimal(str(order["quantity"]))
    previous_levels = {}
    if previous is not None and (raw["marketId"], raw["exchangeId"]) == (
        previous[0]["marketId"],
        previous[0]["exchangeId"],
    ):
        previous_levels = {
            (side, level.price, level.quantity, level.own_quantity): level
            for side in ("bids", "asks")
            for level in getattr(previous[2], side)
        }
    values = {}
    seen = set()
    for name in ("bids", "asks"):
        levels = []
        for row in raw[name]:
            key = (name, Decimal(str(row["price"])))
            if key in seen:
                raise SchemaMismatch("MM_DUPLICATE_BOOK_LEVEL")
            seen.add(key)
            qty = Decimal(str(row["quantity"]))
            own_qty = owned.get(key, ZERO)
            if own_qty > qty:
                raise SchemaMismatch("MM_OWN_ORDER_BOOK_MISMATCH")
            # These immutable numeric fields were already validated. Fresh
            # headers, scope, own-order times, duplicate levels and changed
            # amounts still pass the same checks on every observation.
            previous_level = previous_levels.get((name, key[1], qty, own_qty))
            levels.append(
                previous_level
                if previous_level is not None
                else {"price": str(key[1]), "quantity": str(qty), "own_quantity": str(own_qty)}
            )
        values[name] = levels
    if set(owned) - seen:
        raise SchemaMismatch("MM_OWN_ORDERS_OUTSIDE_VISIBLE_DEPTH")
    return outcome_book(
        Book.model_validate(
            {
                **values,
                "received_at": received,
                "source_at": raw["asOf"]["at"],
                "sequence": raw["asOf"]["sequence"],
                "quality": "HEALTHY",
                "own_orders_known": True,
            }
        ),
        outcome,
    )


def evaluate_actual_book(book, now, tick, cfg, trade_count, window_complete, activity_until):
    """Actual book drives the shared algorithm; no fabricated fee, position or approval."""
    reasons = [
        "MM_FEE_UNVERIFIED",
        "MM_RULES_MAPPING_REVIEW_REQUIRED",
        "MM_ACCOUNT_CHECKPOINT_REQUIRED",
        "MM_LIVE_AUTHORIZATION_REQUIRED",
    ]
    result: dict = {
        "engine": "MM_SIG_V1_CORE_ACTUAL_BOOK",
        "bid": None,
        "ask": None,
        "inventory_assumption": "ZERO_FOR_PRICE_RESEARCH; ACTUAL_INVENTORY_NOT_IMPORTED",
        "known_net_spread": None,
        "blocking_reasons": reasons,
    }
    if not window_complete or not 0 <= (now - activity_until).total_seconds() <= 60:
        reasons.append("MM_ACTIVITY_WINDOW_INCOMPLETE")
    elif trade_count < cfg.min_recent_trades:
        reasons.append("MM_NO_MEASURED_TRADING_DEMAND")
    try:
        buy, sell, _, _ = quote_prices(book, ZERO, tick, now, cfg)
        fee_ceiling = (sell - buy - cfg.exit_cost_buffer - cfg.min_net_spread) / 2
        result.update(
            bid=str(buy),
            ask=str(sell),
            tick=str(tick),
            gross_quote_spread=str(sell - buy),
            maximum_fee_per_share_for_policy=str(fee_ceiling),
        )
        if fee_ceiling < ZERO:
            reasons.append("MM_SPREAD_DOES_NOT_COVER_COST_BUFFER")
    except RiskRejected as exc:
        reasons.append(exc.code)
    return result


def run_scan(args, contract_file, transport=None, *, report_root=None, read_root=None):
    if not args.authorize_read:
        raise AuthorizationRequired("MM_EXPLICIT_READ_AUTHORIZATION_REQUIRED")
    if (
        not 1 <= args.sample_markets <= 5
        or not 2 <= args.samples <= 5
        or not 16 <= args.max_requests <= 80
        or not 1 <= args.seconds <= 180
    ):
        raise ValueError("invalid MM bounded scan limits")
    mm_contract, schema = reviewed_schema()
    cfg = MmConfig.model_validate_json(Path("configs/strategies/mm_sig_v1.json").read_text())
    description = schema.spec["components"]["schemas"]["OrderInput"]["properties"]["price"][
        "description"
    ]
    grid = re.search(r"on the ([0-9]+\.[0-9]+) tick", description)
    if grid is None:
        raise ContractNotVerified("MM_OFFICIAL_PRICE_TICK_UNAVAILABLE")
    tick = Decimal(grid[1])
    directory = report_root or Path("reports/strategies/mm_sig_v1")
    report: dict = {
        "layer": "MOCK" if transport is not None else "REAL_READ",
        "scope": "FULL_CATALOG_AND_BULK_QUOTES; BOUNDED_BOOK_AND_ONE_HOUR_TAPE_SAMPLES",
        "status": "BLOCKED",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "catalog_complete": False,
        "catalog_markets": 0,
        "quote_coverage": 0,
        "samples": [],
        "read_attempts": 0,
        "public_reference_get_attempts": 1,
        "real_orders": 0,
        "real_cancels": 0,
        "account_modifications": 0,
        "trading_enabled": False,
        "strategy_eligible_quotes": 0,
        "fee_model": "UNKNOWN",
        "strategy_profit": "NOT_MEASURED",
        "maker_fill_rate": "NOT_MEASURED",
        "historical_snapshot_used_as_live_book": False,
    }
    reads = None
    try:
        report["official_reference"] = current_reference(directory, schema, transport)
        reads = ReadCheck(
            "MOCK_KEY" if transport is not None else load_key(args.env_file),
            tournament_slug=args.tournament_slug,
            root=read_root or Path("runtime/readonly"),
            max_requests=args.max_requests,
            seconds=args.seconds,
            max_pages=10,
            transport=transport,
            contract_file=contract_file,
        )
        reads.reader.schema = schema
        reads.private = reads.root / "strategies/mm_sig_v1/sessions" / reads.run_id
        evidence = {
            "source": "OFFICIAL",
            "evidence_path": schema.path,
            "sha256": hashlib.sha256(Path(schema.path).read_bytes()).hexdigest(),
            "source_url": OFFICIAL_ORIGIN + "/api/v1/docs",
            "observed_at": mm_contract["reviewed_on"],
            "review_policy": mm_contract["review_scope"],
        }
        for name, cap in reads.reader.contract.data["capabilities"].items():
            if name not in mm_contract["read_capabilities"]:
                continue
            if cap.get("http_method") == "GET" and cap.get("status") in {"DOCUMENTED", "VERIFIED"}:
                if cap["openapi_path"] not in schema.spec["paths"]:
                    raise ContractNotVerified("MM_REVIEWED_READ_PATH_MISSING")
                cap["evidence"] = [deepcopy(evidence)]
                reads.reader.contract.require_documented_read(name)
        account = reads.call("account_identity")
        reads.reader.config.expected_account_id = account["id"]
        report["api_account_bound"] = True

        def pages(name, query):
            reads.checks[name] = {}
            return reads.pages(name, query=query)[0]

        cups = pages("tournament_list", {"status": "any", "limit": 100, "offset": 0})
        matches = [c for c in cups if c["slug"] == args.tournament_slug]
        if len(matches) != 1:
            raise AuthorizationRequired("MM_TOURNAMENT_NOT_UNIQUE")
        tid = matches[0]["id"]
        reads.reader.config.expected_tournament_id = tid
        reads.reader.config.expected_tournament_slug = args.tournament_slug
        cup = reads.call("tournament_identity", path_params={"slug": args.tournament_slug})
        report["tournament_active"] = cup["status"] == "active"
        scoped = {"tournamentId": tid}
        markets = pages("market_list", {**scoped, "status": "any", "limit": 100})
        mapping = {e["id"]: m for m in markets for e in m["exchanges"]}
        if len(mapping) != sum(len(m["exchanges"]) for m in markets):
            raise SchemaMismatch("MM_EXCHANGE_MAPPING_AMBIGUOUS")
        report.update(
            catalog_complete=True,
            catalog_markets=len(markets),
            catalog_pages=reads.checks["market_list"]["pages_observed"],
            tournament_slug=args.tournament_slug,
        )
        prices = []
        ids = list(mapping)
        for start in range(0, len(ids), 100):
            body = reads.call(
                "bulk_prices", query={**scoped, "ids": ",".join(ids[start : start + 100])}
            )
            if any(r["marketId"] != mapping[r["exchangeId"]]["id"] for r in body["data"]):
                raise SchemaMismatch("MM_PRICE_MARKET_MAPPING_MISMATCH")
            prices += body["data"]
        report["quote_coverage"] = len(prices)
        ranked = sorted(
            [
                r
                for r in prices
                if r["bestBid"] is not None
                and r["bestAsk"] is not None
                and 0 < Decimal(str(r["bestBid"])) < Decimal(str(r["bestAsk"])) < 1
            ],
            key=lambda r: Decimal(str(r["bestAsk"])) - Decimal(str(r["bestBid"])),
            reverse=True,
        )
        selected = []
        for row in ranked:
            if mapping[row["exchangeId"]]["status"] == "open":
                selected.append(row["exchangeId"])
            if len(selected) == args.sample_markets:
                break
        report["selected_exchanges"] = selected
        if not selected:
            raise SchemaMismatch("MM_NO_TWO_SIDED_SAMPLE_AVAILABLE")
        before = pages("open_orders", {**scoped, "status": "open", "limit": 200})
        histories = {}
        # Exact whole-second bounds survive the API's observed millisecond formatting.
        until = datetime.now(timezone.utc).replace(microsecond=0)
        for eid in selected:
            histories[eid] = tape(
                reads, mapping[eid]["id"], eid, tid, until - timedelta(hours=1), until, max_pages=3
            )
        for _ in range(args.samples):
            for eid in selected:
                raw = reads.call(
                    "orderbook", path_params={"id": eid}, query={**scoped, "depth": 200}
                )
                received = datetime.now(timezone.utc)
                if raw["marketId"] != mapping[eid]["id"]:
                    raise SchemaMismatch("MM_BOOK_MARKET_MAPPING_MISMATCH")
                book = normalized_book(raw, before, received)
                bid, ask = external_best(book)
                activity_complete = histories[eid]["pagination_complete"] and all(
                    c["complete"] for c in histories[eid]["coverage"]
                )
                report["samples"].append(
                    {
                        "market_id": raw["marketId"],
                        "exchange_id": eid,
                        "received_at": received.isoformat(),
                        "source_at": book.source_at.isoformat(),
                        "sequence": book.sequence,
                        "title": mapping[eid]["title"],
                        "external_best_bid": str(bid),
                        "external_best_ask": str(ask),
                        "spread": str(ask - bid),
                        "spread_fails_minimum_even_before_fees": ask - bid - cfg.exit_cost_buffer
                        < cfg.min_net_spread,
                        "one_hour_trades": len(histories[eid]["trades"]),
                        "activity_window_complete": activity_complete,
                        "own_orders_excluded": True,
                        "rules_reviewed": False,
                        "strategy_evaluation": evaluate_actual_book(
                            book,
                            received,
                            tick,
                            cfg,
                            len(histories[eid]["trades"]),
                            activity_complete,
                            until,
                        ),
                    }
                )
        after = pages("open_orders", {**scoped, "status": "open", "limit": 200})
        stable = content_hash(sorted(before, key=lambda o: o["id"])) == content_hash(
            sorted(after, key=lambda o: o["id"])
        )
        report["own_orders_stable_across_scan"] = stable
        report["own_order_checkpoint"] = (
            "TWO_STABLE_READS_NONATOMIC" if stable else "CHANGED_REJECT_SAMPLES"
        )
        report["open_orders_observed"] = len(after)
        for sample in report["samples"]:
            if not stable:
                sample["strategy_evaluation"]["blocking_reasons"].append(
                    "MM_OWN_ORDER_CHECKPOINT_CHANGED"
                )
        report["price_evaluations"] = sum(
            s["strategy_evaluation"]["bid"] is not None for s in report["samples"]
        )
        report["strategy_eligible_quotes"] = sum(
            not s["strategy_evaluation"]["blocking_reasons"] for s in report["samples"]
        )
        expected = len(selected) * args.samples
        report["status"] = (
            "PASS_READ_ONLY"
            if stable and len(report["samples"]) == expected and len(prices) == len(mapping)
            else "PARTIAL"
        )
        report["market_making_readiness"] = (
            "BLOCKED_FEES_RULE_REVIEW_CONTINUOUS_OBSERVATION_AND_LIVE_AUTHORIZATION"
        )
    except SigbotError as exc:
        report["status"] = "PARTIAL" if report["samples"] else "BLOCKED"
        report["blocker"] = exc.code
    finally:
        if reads is not None:
            report["read_attempts"] = reads.budget.count
            report["requests"] = reads.requests
            report["private_evidence"] = str(reads.private)
            reads.reader.close()
            reads.key = ""
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
    return report
