"""Deterministic, evidence-bound event matching; no operator signature is invented.

Matching an election predicate and matching every settlement exception are separate
facts. The former can be verified from structured SIG contract nodes and external
rules. Unknown exception semantics never become a live approval.
"""

from __future__ import annotations

import re

from sigbot.domain import content_hash
from sigbot.strategies.ecr_sig_v1.publicdata import geography, matched, party


def party_winner_question(text: str) -> bool:
    return bool(
        re.fullmatch(
            r"Will (?:the )?(?:Democrats?|Republicans?|(?:Democratic|Republican) Party(?: candidate)?) win (?:the )?.+\?",
            text,
            re.I,
        )
    )


def sig_predicate(row: dict, nodes: dict, tournament_id: str) -> tuple[dict | None, str]:
    if row.get("is_composite") or row.get("is_multi_outcome"):
        return None, "COMPOSITE_OR_MULTI_OUTCOME"
    if nodes.get("market_id") != row["api_market_id"]:
        return None, "SIG_NODE_MARKET_ID_MISMATCH"
    contexts = nodes.get("contexts", [])
    if len(contexts) != 1 or contexts[0].get("tournament", {}).get("id") != tournament_id:
        return None, "SIG_NODE_TOURNAMENT_MISMATCH"
    root = nodes.get("root") or {}
    details = root.get("contract_details") or {}
    if root.get("node_type") != "contract" or root.get("contract_type") != "Election Outcome":
        return None, "NON_ELECTION_CONTRACT"
    if details.get("resolutionType") != "Party Winner":
        return None, "NOT_PARTY_WINNER"
    if details.get("raceStage") != "General" or details.get("electionDate") != "2026-11-03":
        return None, "NOT_2026_GENERAL_ELECTION"
    subject = {"Democratic Party": "DEMOCRATIC", "Republican Party": "REPUBLICAN"}.get(
        str(details.get("winnerName", ""))
    )
    title_geo = geography(row["title"])
    race_geo = geography(re.sub(r"U\.S\.\s*", "", details.get("raceName", "")))
    if re.search(r"\bspecial\b", details.get("raceName", ""), re.I):
        return None, "SPECIAL_ELECTION_NEEDS_EXPLICIT_SEAT_MAPPING"
    if (
        not subject
        or party(row["title"]) != subject
        or not title_geo
        or not party_winner_question(row["title"])
    ):
        return None, "SIG_PARTY_OR_GEOGRAPHY_UNVERIFIED"
    state, office, district = title_geo
    if details.get("usState") != state or not details.get("raceId") or not details.get("stageId"):
        return None, "SIG_STRUCTURED_EVENT_MISSING"
    # State senate races are not federal Senate elections. Never trust the title alone.
    if office in {"HOUSE", "SENATE"} and details.get("officeLevel") != "Federal":
        return None, "NON_FEDERAL_CONGRESSIONAL_RACE"
    if office == "GOVERNOR" and details.get("officeLevel") != "State":
        return None, "SIG_GOVERNOR_OFFICE_LEVEL_CONFLICT"
    if race_geo is None:
        return None, "SIG_STRUCTURED_RACE_GEOGRAPHY_MISSING"
    if race_geo != title_geo:
        return None, "SIG_TITLE_STRUCTURED_RACE_CONFLICT"
    if len(row.get("exchange_ids", [])) != 1 or row.get("exchange_options") != {
        row["exchange_ids"][0]: "YES"
    }:
        return None, "SIG_BINARY_EXCHANGE_SELECTOR_UNVERIFIED"
    return {
        "year": 2026,
        "state": state,
        "office": office,
        "district": district,
        "election_kind": "GENERAL",
        "party": subject,
        "sig_race_id": str(details["raceId"]),
        "sig_stage_id": str(details["stageId"]),
        "event_key": "EVENT-" + content_hash([2026, details["raceId"], details["stageId"]])[:24],
        "predicate": "PARTY_WIN",
        "payout_yes": "1",
        "payout_no": "0",
    }, "STRUCTURED_SIG_EVENT_VERIFIED"


def external_predicate(contract: dict) -> tuple[dict | None, str]:
    text = "\n".join(
        str(contract.get(k) or "") for k in ("description", "rules_primary", "rules_secondary")
    )
    low = text.casefold()
    if not text.strip():
        return None, "EXTERNAL_RULES_MISSING"
    if re.search(
        r"margin of victory|margin bracket|vote share|percentages of.*votes|popular vote", low
    ):
        return None, "EXTERNAL_RULES_ARE_MARGIN_OR_VOTE_SHARE"
    if re.search(r"\b(primary|nomination|nominee selection)\b", low):
        return None, "EXTERNAL_RULES_ARE_PRIMARY"
    if not re.search(r"\b2026\b", text) or not re.search(r"winner|wins?\b|elected", low):
        return None, "EXTERNAL_WINNER_PREDICATE_UNVERIFIED"
    # Discovery's title parser deliberately rejects words such as "nominee" and
    # "runoff". Rules legitimately discuss both in later paragraphs. Match the
    # explicit election in the opening paragraph, not the whole exceptions text.
    opening = text.split("\n\n", 1)[0]
    if set(re.findall(r"\b20\d{2}\b", opening)) != {"2026"}:
        return None, "EXTERNAL_ELECTION_YEAR_CONFLICT_OR_MISSING"
    if re.search(r"\bspecial\b", opening, re.I):
        return None, "EXTERNAL_SPECIAL_ELECTION_NEEDS_EXPLICIT_SEAT_MAPPING"
    if not re.match(
        r"(?:This market will resolve according to the winner of the|Winner of the)\s+2026\b",
        opening,
        re.I,
    ):
        return None, "EXTERNAL_WINNER_RULE_FORM_UNVERIFIED"
    opening = re.sub(r"U\.S\.\s*", "", opening)
    opening = re.sub(r",?\s*inclusive of any run.?offs.*", "", opening, flags=re.I)
    geo = geography(opening)
    if geo is None or tuple(contract["geography"]) != geo:
        return None, "EXTERNAL_RULES_GEOGRAPHY_CONFLICT_OR_MISSING"
    title_geo = geography(contract.get("title", ""))
    if (
        title_geo != geo
        or party(contract.get("title", "")) != contract.get("party")
        or not party_winner_question(contract.get("title", ""))
    ):
        return None, "EXTERNAL_QUESTION_IDENTITY_CONFLICT_OR_MISSING"
    if contract["operator"] == "POLYMARKET":
        tokens = contract.get("outcome_tokens")
        if (
            not isinstance(tokens, dict)
            or set(tokens) != {"YES", "NO"}
            or any(not isinstance(t, str) or not t for t in tokens.values())
            or tokens["YES"] == tokens["NO"]
            or contract.get("outcome_side") != "YES"
            or contract.get("contract_id") != tokens["YES"]
            or contract.get("no_token") != tokens["NO"]
            or not contract.get("condition_id")
            or not contract.get("market_id")
        ):
            return None, "EXTERNAL_OUTCOME_TOKEN_BINDING_UNVERIFIED"
    # The common winner rule must define party representation by nomination;
    # a candidate name or an incidental party mention is not a party-win payoff.
    party_definition = bool(
        re.search(r"(?:represents?|represent).*?party.*?nominee|nominee.*?represents?.*?party", low)
    )
    if not party_definition:
        return None, "EXTERNAL_PARTY_PREDICATE_UNVERIFIED"
    return {
        "year": 2026,
        "state": geo[0],
        "office": geo[1],
        "district": geo[2],
        "election_kind": "GENERAL",
        "party": contract["party"],
        "outcome_side": "YES",
        "predicate": "PARTY_WIN",
        "runoff_included": bool(
            re.search(r"inclusive of any run.?offs|includes? .*run.?offs", low)
        ),
        "replacement_nominee_included": "replacement nominee" in low,
        "rules_hash": content_hash(text),
    }, "EXTERNAL_WINNER_RULES_VERIFIED"


def compare(row: dict, nodes: dict, contract: dict, tournament_id: str) -> dict:
    local, local_reason = sig_predicate(row, nodes, tournament_id)
    remote, remote_reason = external_predicate(contract)
    fields = ("year", "state", "office", "district", "election_kind", "party", "predicate")
    same = bool(local and remote and all(local[k] == remote[k] for k in fields))
    reason = (
        local_reason
        if local is None
        else remote_reason
        if remote is None
        else ("EVENT_PREDICATE_MATCH" if same else "EVENT_PREDICATE_MISMATCH")
    )
    return {
        "api_market_id": row["api_market_id"],
        "exchange_ids": row.get("exchange_ids", []),
        "page_id": row.get("page_id"),
        "operator": contract["operator"],
        "external_contract_id": contract["contract_id"],
        "external_market_id": contract.get("market_id"),
        "external_condition_id": contract.get("condition_id"),
        "external_no_token": contract.get("no_token"),
        "sig_rules_hash": content_hash(nodes),
        "external_rules_hash": content_hash(
            {
                k: contract.get(k)
                for k in (
                    "operator",
                    "contract_id",
                    "market_id",
                    "condition_id",
                    "no_token",
                    "outcome_side",
                    "outcome_tokens",
                    "title",
                    "description",
                    "resolution_source",
                    "rules_primary",
                    "rules_secondary",
                )
            }
        ),
        "event": local,
        "external_event": remote,
        "predicate_match": same,
        "reason": reason,
        "verification_method": "STRUCTURED_SIG_FIELDS_AND_OFFICIAL_EXTERNAL_RULE_TEXT",
        "settlement_equivalent": False,
        "settlement_gaps": [
            "SIG_REPLACEMENT_NOMINEE_RULE_NOT_EXPOSED",
            "SIG_CONFIGURED_RESOLUTION_SOURCE_NOT_EXPOSED",
            "CROSS_VENUE_NA_AND_DELAY_RULES_NOT_PROVEN_IDENTICAL",
        ]
        if same
        else [],
        "requires_operator_signature_for_predicate_match": False,
        "authorized_for_live": False,
    }


def correspondences(
    rows: list[dict], nodes: dict[str, dict], contracts: list[dict], tid: str
) -> list[dict]:
    result: list[dict] = []
    for row in rows:
        if row["api_market_id"] not in nodes:
            continue
        candidates = matched(row, contracts)
        result.extend(compare(row, nodes[row["api_market_id"]], c, tid) for c in candidates)
    # Multiple verified contracts on one operator remain ambiguous. Do not pick the best price.
    for item in result:
        same_operator = [
            x
            for x in result
            if x["api_market_id"] == item["api_market_id"]
            and x["operator"] == item["operator"]
            and x["predicate_match"]
        ]
        item["unique_operator_match"] = len(same_operator) == 1 and item["predicate_match"]
        if item["predicate_match"] and not item["unique_operator_match"]:
            item["reason"] = "MULTIPLE_WINNER_CONTRACTS_ON_SAME_OPERATOR"
        identities = {
            (x["event"]["event_key"], x["event"]["party"])
            for x in result
            if x["predicate_match"]
            and x["operator"] == item["operator"]
            and x["external_contract_id"] == item["external_contract_id"]
        }
        if len(identities) > 1:
            item["unique_operator_match"] = False
            item["reason"] = "EXTERNAL_TOKEN_BINDS_MULTIPLE_SIG_EVENTS"
    return result
