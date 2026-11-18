"""Source-preserving registries and explicit, version-bound rule review."""

from __future__ import annotations

import csv
import hashlib
import itertools
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse

from sigbot.domain import content_hash, utc_time
from sigbot.errors import SchemaMismatch
from sigbot.strategies.prc_sig_v1.catalog import catalog_rows

SOURCE = Path("data/semantic_market_snapshot")
REQUIRED_FACTS = (
    "same_seat_cycle",
    "winner_definition",
    "independent_other_mapping",
    "cancellation_policy",
    "observation_settlement_times",
    "resolution_source",
    "binary_side_mapping",
    "price_quantity_units",
)


def read_csv(path):
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def write_json(path, value):
    path = Path(path)
    if path.resolve().is_relative_to(Path("sources").resolve()):
        raise SchemaMismatch("SEMANTIC_SOURCE_MUTATION_FORBIDDEN")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")


def inputs(source=SOURCE):
    """Check every supplied asset before trusting the source-derived registry."""
    source = Path(source)
    manifest = json.loads((source / "MANIFEST.sha256.json").read_text())
    for name, digest in manifest.items():
        path = source / name
        if not path.resolve().is_relative_to(source.resolve()):
            raise SchemaMismatch("SEMANTIC_MANIFEST_PATH_INVALID")
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise SchemaMismatch("SEMANTIC_SOURCE_HASH_MISMATCH")
    markets = read_csv(source / "data/market_registry.csv")
    # Independently count the complete quote blocks, not the duplicated UI titles.
    text = (source / "data/source_market_snapshot.txt").read_text(encoding="utf-8-sig")
    titles = re.findall(
        r"^(Will the [^\n]+\?)\nYes\n\d+(?:\.\d+)?%\n·\nNo\n\d+(?:\.\d+)?%", text, re.M
    )
    if Counter(titles) != Counter(m["market_title"] for m in markets):
        raise SchemaMismatch("SEMANTIC_SOURCE_REGISTRY_DIFF")
    if len({m["local_market_key"] for m in markets}) != len(markets):
        raise SchemaMismatch("SEMANTIC_LOCAL_KEY_COLLISION")
    return markets


def map_catalog(local, raw, tournament_id, receipts):
    official = catalog_rows(raw, tournament_id)  # Existing ID/context collision checks.
    by_title = defaultdict(list)
    for item in official:
        by_title[" ".join(item["title"].split())].append(item)
    result = []
    for market in local:
        matches = by_title[" ".join(market["market_title"].split())]
        row = {
            **market,
            "sig_market_id": None,
            "exchange_id": None,
            "tournament_id": tournament_id,
            "mapping_status": "MISSING_OFFICIAL_TITLE",
            "rules_hash": None,
            "trade_enabled": False,
            "data_provenance": "USER_OLD_DISPLAY_SNAPSHOT",
            "catalog_receipts": receipts,
        }
        if len(matches) > 1:
            row["mapping_status"] = "AMBIGUOUS_OFFICIAL_TITLE"
        elif matches:
            item = matches[0]
            row["sig_market_id"] = item["api_market_id"]
            row["official_payload_hash"] = item["raw_hash"]
            row["official_status"] = item["status"]
            row["official_settlement_date"] = item["settlement_date"]
            if (
                len(item["exchange_ids"]) == 1
                and item["exchange_options"][item["exchange_ids"][0]] == "YES"
                and not item["is_multi_outcome"]
                and not item["is_composite"]
            ):
                row["exchange_id"] = item["exchange_ids"][0]
                row["mapping_status"] = "EXACT_OFFICIAL_TITLE_AND_EXCHANGE"
            else:
                row["mapping_status"] = "EXCHANGE_SEMANTICS_UNVERIFIED"
        result.append(row)
    matched = {r["sig_market_id"] for r in result} - {None}
    return result, {
        "mapped": sum(r["mapping_status"] == "EXACT_OFFICIAL_TITLE_AND_EXCHANGE" for r in result),
        "statuses": dict(Counter(r["mapping_status"] for r in result)),
        "official_only_market_ids": sorted(
            r["api_market_id"] for r in official if r["api_market_id"] not in matched
        ),
        "event_identity_approved": 0,
        "title_match_is_rule_proof": False,
    }


def record_rules(directory, item):
    """Archive an operator-provided official Info artifact; never execute its text."""
    url = urlparse(item["source_url"])
    if url.scheme != "https" or url.hostname != "sig.thesuper.market":
        raise SchemaMismatch("SEMANTIC_RULE_SOURCE_NOT_OFFICIAL")
    utc_time(item["received_at"])
    if (
        not item.get("text", "").strip()
        or not item.get("sig_market_id")
        or not item.get("tournament_id")
    ):
        raise SchemaMismatch("SEMANTIC_RULE_TEXT_OR_ID_MISSING")
    digest = hashlib.sha256(item["text"].encode()).hexdigest()
    if item.get("rules_hash", digest) != digest:
        raise SchemaMismatch("SEMANTIC_RULE_HASH_MISMATCH")
    value = {**item, "rules_hash": digest, "verification_status": "RULES_FETCHED"}
    folder = Path(directory)
    if folder.resolve().is_relative_to(Path("sources").resolve()):
        raise SchemaMismatch("SEMANTIC_SOURCE_MUTATION_FORBIDDEN")
    folder.mkdir(parents=True, exist_ok=True)
    # Content-addressed version; changed rules create a new artifact, never replace it.
    path = folder / (content_hash(value) + ".json")
    if not path.exists():
        with path.open("x") as out:
            json.dump(value, out, ensure_ascii=False, indent=2)
    return value


def latest_rules(directory, at):
    values: dict = {}
    for path in Path(directory).glob("*.json"):
        item = json.loads(path.read_text())
        if path.stem != content_hash(item):
            raise SchemaMismatch("SEMANTIC_RULE_VERSION_METADATA_CORRUPTED")
        if hashlib.sha256(item["text"].encode()).hexdigest() != item["rules_hash"]:
            raise SchemaMismatch("SEMANTIC_RULE_ARCHIVE_CORRUPTED")
        received = utc_time(item["received_at"])
        if received > at:
            continue
        mid = item["sig_market_id"]
        previous = values.get(mid)
        if (
            previous
            and received == utc_time(previous["received_at"])
            and item["rules_hash"] != previous["rules_hash"]
        ):
            raise SchemaMismatch("SEMANTIC_CONFLICTING_RULE_VERSIONS")
        if previous is None or received > utc_time(previous["received_at"]):
            values[mid] = item
    return values


def review_status(members, rules, review, at):
    if any(m.get("mapping_status") != "EXACT_OFFICIAL_TITLE_AND_EXCHANGE" for m in members):
        return "TITLE_CANDIDATE", ["ID_MAPPING_UNVERIFIED"]
    hashes = {
        m["sig_market_id"]: rules.get(m["sig_market_id"], {}).get("rules_hash") for m in members
    }
    if any(h is None for h in hashes.values()):
        return "TITLE_CANDIDATE", ["MISSING_CONTRACT_INFO_RULES"]
    if any(
        rules[m["sig_market_id"]].get("tournament_id") != m.get("tournament_id") for m in members
    ):
        return "REVIEW_REQUIRED", ["RULE_SCOPE_MISMATCH"]
    if any(
        rules[m["sig_market_id"]].get("received_at")
        and utc_time(rules[m["sig_market_id"]]["received_at"]) > at
        for m in members
    ):
        return "REVIEW_REQUIRED", ["RULE_NOT_YET_AVAILABLE"]
    if any(rules[m["sig_market_id"]].get("fusion_ticket_multiple_yes") is True for m in members):
        return "REJECTED_BY_RULES", ["OFFICIAL_RULES_ALLOW_MULTIPLE_PARTY_YES"]
    if not review:
        return "RULES_FETCHED", ["HUMAN_REVIEW_REQUIRED"]
    bindings = {m["sig_market_id"]: m["exchange_id"] for m in members}
    if review.get("rules_hashes") != hashes or review.get("exchange_bindings") != bindings:
        return "REVIEW_REQUIRED", ["RULE_OR_MAPPING_CHANGED"]
    if review.get("verified_at") and any(
        rules[m["sig_market_id"]].get("received_at")
        and utc_time(review["verified_at"]) < utc_time(rules[m["sig_market_id"]]["received_at"])
        for m in members
    ):
        return "REVIEW_REQUIRED", ["REVIEW_PRECEDES_RULE_RECEIPT"]
    if (
        not review.get("verified_by")
        or not review.get("verified_at")
        or utc_time(review["verified_at"]) > at
        or not review.get("effective_from")
        or not review.get("effective_to")
        or not utc_time(review["effective_from"]) <= at < utc_time(review["effective_to"])
    ):
        return "REVIEW_REQUIRED", ["REVIEW_NOT_AVAILABLE_OR_EXPIRED"]
    if any(
        not isinstance(review.get("facts", {}).get(k), str) or not review["facts"][k].strip()
        for k in REQUIRED_FACTS
    ):
        return "REVIEW_REQUIRED", ["INCOMPLETE_RULE_FACTS"]
    if (
        review.get("mutual_exclusion_verified") is not True
        or review.get("payoff_tests_passed") is not True
    ):
        return "HUMAN_REVIEWED", ["PAYOFF_PROOF_REQUIRED"]
    return "VERIFIED", []


def groups(markets):
    races = defaultdict(list)
    for market in markets:
        races[market["race_id"]].append(market)
    return dict(races)


def bundles(members):
    # Pair and triple scans are alternative uses of the same liquidity, never allocations.
    for size in range(2, len(members) + 1):
        yield from itertools.combinations(
            sorted(members, key=lambda m: m["local_market_key"]), size
        )
