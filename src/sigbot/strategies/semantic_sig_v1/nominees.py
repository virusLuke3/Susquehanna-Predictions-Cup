"""Bind official candidate-list observations to rules; never sign an operator review."""

from __future__ import annotations

import hashlib
import itertools
import json
import re
import subprocess
from pathlib import Path
from urllib.parse import urlparse

from sigbot.domain import content_hash, utc_time
from sigbot.errors import SchemaMismatch
from sigbot.strategies.semantic_sig_v1.core import payouts
from sigbot.strategies.semantic_sig_v1.registry import groups, write_json


def iowa_candidates(text):
    if "November 3, 2026 General Election" not in text or "Ballot Name(s)" not in text:
        raise SchemaMismatch("SEMANTIC_CANDIDATE_LIST_SCOPE_INVALID")
    offices = {
        "United States Senator": "2026-IA-SENATE",
        "Governor": "2026-IA-GOVERNOR",
        **{
            f"United States Representative District {i}": f"2026-IA-HOUSE-{i:02}"
            for i in range(1, 5)
        },
    }
    rows, race = [], None
    for line_number, line in enumerate(text.split("\f")[0].splitlines(), 1):
        match = re.match(r"^(.*?)\s{2,}(Republican|Democratic|Libertarian|No Party)\s+(.+)$", line)
        if not match:
            continue
        office, party, rest = match.groups()
        if office.strip():
            race = offices.get(office.strip())
        if race is None:
            continue
        name = re.split(r"\s{2,}", rest.strip())[0]
        if name == "--" or not name:
            raise SchemaMismatch("SEMANTIC_CANDIDATE_PLACEHOLDER_NOT_NOMINEE")
        rows.append(
            {
                "race_id": race,
                "candidate_name": name,
                "party_label": party,
                "party_code": {"Republican": "R", "Democratic": "D"}.get(party, "OTHER"),
                "source_line": line_number,
            }
        )
    if not rows or any(
        not {"D", "R"} <= {r["party_code"] for r in members} for members in groups(rows).values()
    ):
        raise SchemaMismatch("SEMANTIC_CANDIDATE_MAJOR_PARTY_ROWS_MISSING")
    return rows


def michigan_candidates(text):
    if "<tr" in text.lower():
        # The public report is a table; retain row boundaries, not script content.
        from sigbot.strategies.semantic_sig_v1.official_rules import PageText

        lines = []
        for row in re.findall(r"<tr\b[^>]*>(.*?)</tr>", text, re.I | re.S):
            parser = PageText()
            parser.feed(row)
            lines.append(" ".join(parser.text))
        text = "\n".join(lines)
    if "Official Candidate Listing" not in text or "Tuesday, November 3, 2026" not in text:
        raise SchemaMismatch("SEMANTIC_CANDIDATE_LIST_SCOPE_INVALID")
    offices = {"Governor / Lt. Governor": "2026-MI-GOVERNOR", "U.S. Senate": "2026-MI-SENATE"}
    parties = [
        "Democratic Party",
        "Republican Party",
        "Libertarian Party",
        "U.S. Taxpayers Party",
        "Green Party",
        "Natural Law Party",
        "No Party Affiliation",
    ]
    pattern = (
        "^("
        + "|".join(map(re.escape, parties))
        + r")\s+(.+?)\s+\d{2}/\d{2}/\d{4}\s+(?:Petitions|Convention)$"
    )
    rows, race = [], None
    for line_number, line in enumerate(text.splitlines(), 1):
        line = " ".join(re.sub(r"^L\d+:\s*", "", line).split())
        heading = re.match(r"^(.*?)\s+\d+ Year Term", line)
        if heading:
            race = offices.get(heading[1])
        match = re.match(pattern, line)
        if race and not match and re.search(r"\d{2}/\d{2}/\d{4}", line):
            raise SchemaMismatch("SEMANTIC_CANDIDATE_ROW_NOT_UNDERSTOOD")
        if race and match:
            party, name = match.groups()
            rows.append(
                {
                    "race_id": race,
                    "candidate_name": name,
                    "candidate_identity": name.split("/", 1)[0].strip()
                    if race.endswith("GOVERNOR")
                    else name,
                    "party_label": party,
                    "party_code": {"Republican Party": "R", "Democratic Party": "D"}.get(
                        party, "OTHER"
                    ),
                    "source_line": line_number,
                }
            )
    if set(groups(rows)) != set(offices.values()) or any(
        not {"D", "R"} <= {r["party_code"] for r in members} for members in groups(rows).values()
    ):
        raise SchemaMismatch("SEMANTIC_CANDIDATE_MAJOR_PARTY_ROWS_MISSING")
    return rows


def candidate_packet(packet):
    receipt = packet["source_receipt"]
    url = urlparse(receipt["url"])
    iowa = url.hostname == "sos.iowa.gov" and url.path.endswith(".pdf")
    michigan = (
        url.hostname == "mi-boe.entellitrak.com"
        and url.path == "/etk-mi-boe-prod/page.request.do"
        and (
            receipt.get("retrieval_method") == "WEB_TOOL_RENDERED_TEXT"
            or receipt.get("status_code") == 200
        )
        and receipt.get("official_referral_url") == "https://www.michigan.gov/sos/elections"
    )
    if url.scheme != "https" or not (iowa or michigan):
        raise SchemaMismatch("SEMANTIC_CANDIDATE_SOURCE_NOT_SUPPORTED_OFFICIAL_LIST")
    utc_time(receipt["received_at"])
    raw = Path(receipt["path"]).read_bytes()
    if (iowa and receipt.get("status_code") != 200) or hashlib.sha256(raw).hexdigest() != receipt[
        "sha256"
    ]:
        raise SchemaMismatch("SEMANTIC_CANDIDATE_SOURCE_RECEIPT_INVALID")
    text = Path(packet["text_path"]).read_bytes()
    if hashlib.sha256(text).hexdigest() != packet["text_sha256"]:
        raise SchemaMismatch("SEMANTIC_CANDIDATE_TEXT_CHANGED")
    # Re-run the installed extractor against the bound PDF: a matching text hash
    # alone would not prove that a supplied transcript belongs to this source.
    try:
        extracted = (
            subprocess.run(
                ["pdftotext", "-layout", receipt["path"], "-"],
                check=True,
                capture_output=True,
                timeout=30,
            ).stdout
            if iowa
            else raw
        )
    except (subprocess.SubprocessError, OSError):
        raise SchemaMismatch("SEMANTIC_CANDIDATE_PDF_EXTRACTION_FAILED") from None
    if extracted != text:
        raise SchemaMismatch("SEMANTIC_CANDIDATE_TRANSCRIPT_NOT_FROM_SOURCE")
    return groups((iowa_candidates if iowa else michigan_candidates)(text.decode())), iowa


def candidate_version(rows):
    return content_hash(
        sorted(
            (
                r["race_id"],
                " ".join(r.get("candidate_identity", r["candidate_name"]).casefold().split()),
                " ".join(r["candidate_name"].casefold().split()),
                r["party_label"],
            )
            for r in rows
        )
    )


def listed_affiliations(rows):
    affiliations: dict[str, set] = {}
    for row in rows:
        identity = " ".join(row.get("candidate_identity", row["candidate_name"]).casefold().split())
        affiliations.setdefault(identity, set()).add(row["party_code"])
    return affiliations


def nomination_findings(packet):
    """Check primary documents for limited findings, never infer SIG affiliations."""
    result = {}
    required = {
        "IA": (
            "www.legis.iowa.gov",
            "/docs/code/2026/49.pdf",
            [
                "49.39 Dual nomination.",
                "nominate the same candidate for the same office",
                "49.99 Writing name on ballot.",
                "49.58 Effect of death of certain candidates.",
            ],
        ),
        "MI": (
            "www.legislature.mi.gov",
            "/documents/mcl/pdf/mcl-116-1954-XXVIII.pdf",
            [
                "168.692 Nomination by more than 1 party",
                "certified as a nominee by more than 1 political party",
                "168.695 Ineligibility",
                "shall be eligible as a candidate of any other political party",
            ],
        ),
    }
    for item in packet.get("documents", []):
        receipt = item["source_receipt"]
        state = item["state"]
        if state not in required or state in result:
            raise SchemaMismatch("SEMANTIC_NOMINATION_DOCUMENT_SCOPE_INVALID")
        host, path, phrases = required[state]
        url = urlparse(receipt["url"])
        raw = Path(receipt["path"]).read_bytes()
        utc_time(receipt["received_at"])
        if (
            (url.scheme, url.hostname, url.path) != ("https", host, path)
            or receipt.get("status_code") != 200
            or hashlib.sha256(raw).hexdigest() != receipt["sha256"]
        ):
            raise SchemaMismatch("SEMANTIC_NOMINATION_RECEIPT_INVALID")
        try:
            text = subprocess.run(
                ["pdftotext", "-layout", receipt["path"], "-"],
                check=True,
                capture_output=True,
                timeout=30,
            ).stdout
        except (subprocess.SubprocessError, OSError):
            raise SchemaMismatch("SEMANTIC_NOMINATION_TEXT_UNAVAILABLE") from None
        if hashlib.sha256(text).hexdigest() != item["text_sha256"]:
            raise SchemaMismatch("SEMANTIC_NOMINATION_TRANSCRIPT_CHANGED")
        normalized = " ".join(text.decode().split())
        if any(phrase not in normalized for phrase in phrases):
            raise SchemaMismatch("SEMANTIC_NOMINATION_DOCUMENT_CHANGED")
        result[state] = {
            "source_receipt": receipt,
            "finding": "ONE_BALLOT_LABEL_DOES_NOT_PROVE_ONE_NOMINATION",
            "primary_cross_party_ineligibility_text_observed": state == "MI",
            "unlisted_write_in_and_death_provisions_observed": state == "IA",
            "sig_affiliation_mapping_proven": False,
            "actual_dual_nominee_observed": None,
        }
    return result


def audit_candidates(
    input_path, registry_path, output, *, nomination_evidence=None, previous_audit=None
):
    packet = json.loads(Path(input_path).read_text())
    candidates, iowa = candidate_packet(packet)
    receipt = packet["source_receipt"]
    legal_packet = json.loads(Path(nomination_evidence).read_text()) if nomination_evidence else {}
    findings = nomination_findings(legal_packet)
    previous = json.loads(Path(previous_audit).read_text()) if previous_audit else {}
    prior = {r["race_id"]: r for r in previous.get("races", [])}
    registry = json.loads(Path(registry_path).read_text())
    results = []
    for race, members in groups(registry["markets"]).items():
        if race not in candidates:
            continue
        listed = candidates[race]
        affiliations = listed_affiliations(listed)
        states = sorted({frozenset(v) for v in affiliations.values()}, key=lambda v: sorted(v))
        legs = [
            {"key": m["local_market_key"], "party": m["party_code"], "side": "NO", "quantity": "1"}
            for m in members
        ]
        normal = payouts(legs, states)
        parties = sorted({m["party_code"] for m in members})
        all_states = ["OTHER"] + [
            frozenset(c)
            for n in range(1, len(parties) + 1)
            for c in itertools.combinations(parties, n)
        ]
        robust = payouts(legs, all_states)
        details = [m.get("official_contract_details") or {} for m in members]
        state = "IA" if iowa else "MI"
        state_name = "Iowa" if iowa else "Michigan"
        expected_office = "HOUSE" if "-HOUSE-" in race else race.rsplit("-", 1)[-1]
        expected_name = (
            f"U.S. House {state_name} District {int(race.rsplit('-', 1)[-1])}"
            if expected_office == "HOUSE"
            else f"U.S. Senate {state_name}"
            if expected_office == "SENATE"
            else f"Governor of {state_name}"
        )
        if (
            any(
                d.get("usState") != state
                or d.get("raceName") != expected_name
                or d.get("electionDate") != "2026-11-03"
                or d.get("raceStage") != "General"
                or not d.get("raceId")
                or not d.get("stageId")
                or d.get("resolutionType") != "Party Winner"
                or d.get("winnerName")
                != {"D": "Democratic Party", "R": "Republican Party"}.get(m["party_code"])
                for m, d in zip(members, details, strict=True)
            )
            or any(
                m.get("mapping_status") != "EXACT_OFFICIAL_TITLE_AND_EXCHANGE"
                or m.get("office") != expected_office
                or not re.fullmatch(r"[0-9a-f]{64}", m.get("rules_hash") or "")
                for m in members
            )
            or len({(d.get("raceId"), d.get("stageId")) for d in details}) != 1
        ):
            raise SchemaMismatch("SEMANTIC_CANDIDATE_CONTRACT_SCOPE_MISMATCH")
        results.append(
            {
                "race_id": race,
                "candidates": listed,
                "candidate_version": candidate_version(listed),
                "relation_version": content_hash(
                    [
                        candidate_version(listed),
                        {m["sig_market_id"]: [m["exchange_id"], m["rules_hash"]] for m in members},
                    ]
                ),
                "candidate_list_changed": candidate_version(prior[race]["candidates"])
                != candidate_version(listed)
                if race in prior
                else None,
                "source_observed_at": receipt["received_at"],
                "rules_hashes": {m["sig_market_id"]: m["rules_hash"] for m in members},
                "exchange_bindings": {m["sig_market_id"]: m["exchange_id"] for m in members},
                "claim_scope": "LISTED_CANDIDATES_AND_LISTED_PARTY_LABELS_ONLY",
                "listed_candidate_d_r_overlap": any({"D", "R"} <= v for v in affiliations.values()),
                "candidate_conditioned_normal_payouts": {k: str(v) for k, v in normal.items()},
                "candidate_conditioned_min_payout": str(min(normal.values())),
                "unlisted_winner_payoff": None,
                "all_affiliation_normal_payouts": {k: str(v) for k, v in robust.items()},
                "all_affiliation_normal_min_payout": str(min(robust.values())),
                "unlisted_winner_payout_range": {
                    "min": str(min(robust.values())),
                    "max": str(max(robust.values())),
                },
                "robust_payoff_computation": "COMPLETE_ALL_BINARY_AFFILIATION_STATES",
                "unconditional_two_no_bound_one_claim": "REJECT",
                "nomination_finding": findings.get(state),
                "evidence_review": "COMPLETED_LIMITED_SCOPE_NOT_OPERATOR_EXECUTION_APPROVAL",
                "lifecycle_policy": "CANDIDATE_RULE_OR_EXCHANGE_VERSION_CHANGE_INVALIDATES_PRIOR_RELATION",
                "remote_source_continuously_monitored": False,
                "decision": "NEED_MORE_DATA",
                "trade_enabled": False,
                "remaining_facts": [
                    "SIG_AFFILIATION_AND_FUSION_SCOPE_VERSUS_BALLOT_LABEL",
                    "FULL_NOMINATION_AND_UNLISTED_WINNER_AFFILIATION_FOR_TIGHTER_BOUND",
                ],
                "review_draft": {"verified_by": None, "mutual_exclusion_verified": False},
            }
        )
    report = {
        "version": "CANDIDATE_RESOLUTION_AUDIT_V2",
        "layer": "LOCAL_OFFICIAL_CANDIDATE_DOCUMENT_READBACK",
        "input_packet": packet,
        "nomination_evidence_packet": legal_packet,
        "source_receipt": receipt,
        "text_sha256": packet["text_sha256"],
        "previous_audit_sha256": hashlib.sha256(Path(previous_audit).read_bytes()).hexdigest()
        if previous_audit
        else None,
        "registry_sha256": hashlib.sha256(Path(registry_path).read_bytes()).hexdigest(),
        "races": results,
        "decision": "NEED_MORE_DATA",
        "real_orders": 0,
        "official_list_is_exhaustive_resolution_proof": False,
    }
    write_json(Path(output) / "candidate_rule_audit.json", report)
    return {
        "races_audited": len(results),
        "candidate_rows": sum(len(r["candidates"]) for r in results),
        "decision": "NEED_MORE_DATA",
        "real_orders": 0,
    }


def load_candidate_audits(paths, markets, rules, at):
    """Reparse source facts; stale bindings invalidate only that race's conditional view."""
    evidence = {}
    current = groups(markets)
    for path in paths:
        report = json.loads(Path(path).read_text())
        if report.get("version") != "CANDIDATE_RESOLUTION_AUDIT_V2":
            raise SchemaMismatch("SEMANTIC_CANDIDATE_AUDIT_VERSION_UNSUPPORTED")
        if report["source_receipt"] != report["input_packet"]["source_receipt"]:
            raise SchemaMismatch("SEMANTIC_CANDIDATE_RECEIPT_CHANGED")
        parsed, _ = candidate_packet(report["input_packet"])
        nomination_findings(report.get("nomination_evidence_packet", {}))
        received = utc_time(report["source_receipt"]["received_at"])
        for row in report["races"]:
            race = row["race_id"]
            if race in evidence:
                raise SchemaMismatch("SEMANTIC_CANDIDATE_AUDIT_RACE_CONFLICT")
            if row["candidates"] != parsed.get(race) or row[
                "candidate_version"
            ] != candidate_version(parsed[race]):
                raise SchemaMismatch("SEMANTIC_CANDIDATE_FACTS_NOT_FROM_SOURCE")
            members = current.get(race, [])
            reasons = []
            if received > at:
                reasons.append("CANDIDATE_SOURCE_NOT_YET_AVAILABLE")
            if any(
                utc_time(d["source_receipt"]["received_at"]) > at
                for d in report.get("nomination_evidence_packet", {}).get("documents", [])
                if members
                and d["state"] == (members[0].get("official_contract_details") or {}).get("usState")
            ):
                reasons.append("NOMINATION_DOCUMENT_NOT_YET_AVAILABLE")
            if any(
                rules.get(m["sig_market_id"], {}).get("received_at")
                and utc_time(rules[m["sig_market_id"]]["received_at"]) > at
                for m in members
            ):
                reasons.append("CANDIDATE_RULE_NOT_YET_AVAILABLE")
            if (
                not members
                or row["rules_hashes"]
                != {
                    m["sig_market_id"]: rules.get(m["sig_market_id"], {}).get("rules_hash")
                    for m in members
                }
                or row["exchange_bindings"]
                != {m["sig_market_id"]: m["exchange_id"] for m in members}
                or any(
                    m.get("mapping_status") != "EXACT_OFFICIAL_TITLE_AND_EXCHANGE" for m in members
                )
            ):
                reasons.append("CANDIDATE_RULE_OR_MAPPING_CHANGED")
            evidence[race] = {
                "status": "INVALIDATED" if reasons else "CONDITIONAL_SNAPSHOT_VERIFIED",
                "reasons": reasons,
                "candidates": parsed[race],
                "listed_affiliation_states": [
                    sorted(v)
                    for v in sorted(
                        set(map(frozenset, listed_affiliations(parsed[race]).values())),
                        key=lambda v: sorted(v),
                    )
                ],
                "candidate_version": row["candidate_version"],
                "relation_version": content_hash(
                    [
                        row["candidate_version"],
                        {
                            m["sig_market_id"]: [
                                m["exchange_id"],
                                rules.get(m["sig_market_id"], {}).get("rules_hash"),
                            ]
                            for m in members
                        },
                    ]
                )
                if not reasons
                else None,
                "source_observed_at": report["source_receipt"]["received_at"],
                "audit_sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest(),
                "trade_approval": False,
            }
    return evidence
