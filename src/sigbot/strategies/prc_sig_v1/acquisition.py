"""Bounded anonymous original-poll acquisition; documents never approve trades.

Only verified publisher index URLs are seeded. Release/PDF URLs must be actual
links on those pages. Receipt time is never substituted for first publication.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import re
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qs, urldefrag, urljoin, urlsplit

import httpx

from sigbot.errors import AuthorizationRequired, SchemaMismatch, TransportUnknown
from sigbot.readcheck import private_write

ROOT = Path("runtime/readonly/strategies/prc_sig_v1/data")
SOURCES = {
    "MARIST": "https://maristpoll.marist.edu/latest-polls/",
    "EMERSON": "https://emersoncollegepolling.com/blog/",
    "QUINNIPIAC": "https://poll.qu.edu/poll-results/",
}
MAX_BYTES = 5_000_000


def now():
    return datetime.now(timezone.utc)


def write_json(path: Path, data):
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    private_write(temp, json.dumps(data, ensure_ascii=False, indent=2, default=str).encode())
    temp.replace(path)


def preserve_text(path: Path, text: str):
    if path.exists():
        if path.read_bytes() != text.encode():
            raise SchemaMismatch("PRC_ARCHIVED_TEXT_HASH_COLLISION_OR_PARSER_CHANGE")
    else:
        private_write(path, text.encode())


def allowed_url(source: str, url: str) -> str:
    url = urldefrag(url)[0]
    parsed = urlsplit(url)
    if (
        source not in SOURCES
        or parsed.scheme != "https"
        or parsed.hostname != urlsplit(SOURCES[source]).hostname
        or parsed.username
        or parsed.password
        or parsed.port not in (None, 443)
        or re.search(r"login|signin|wp-admin|wp-json|account", parsed.path, re.I)
        or (
            parsed.query
            and not (
                source == "QUINNIPIAC"
                and parsed.path == "/poll-release"
                and re.fullmatch(r"releaseid=\d+", parsed.query)
            )
        )
    ):
        raise SchemaMismatch("PRC_PUBLIC_URL_OUTSIDE_VERIFIED_SOURCE")
    return url


class Page(HTMLParser):
    def __init__(self, raw: str):
        super().__init__(convert_charrefs=True)
        self.links: list[dict] = []
        self.meta: dict[str, list[str]] = {}
        self.text: list[str] = []
        self.headings: list[str] = []
        self.anchor: dict | None = None
        self.hidden = 0
        self.heading = False
        self.feed(raw)

    def handle_starttag(self, tag, attrs):
        data = dict(attrs)
        if tag in {"script", "style"}:
            self.hidden += 1
        if tag == "meta":
            key = data.get("property") or data.get("name")
            if key and data.get("content"):
                self.meta.setdefault(key, []).append(data["content"])
        if tag == "a" and data.get("href"):
            self.anchor = {"href": data["href"], "label": ""}
        if tag == "h1":
            self.heading = True

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self.hidden = max(0, self.hidden - 1)
        if tag == "a" and self.anchor:
            self.links.append(self.anchor)
            self.anchor = None
        if tag == "h1":
            self.heading = False

    def handle_data(self, data):
        if self.hidden:
            return
        value = " ".join(data.split())
        if value:
            self.text.append(value)
            if self.heading:
                self.headings.append(value)
            if self.anchor:
                self.anchor["label"] += value + " "

    @property
    def title(self):
        return " ".join(self.headings) or next(iter(self.meta.get("og:title", [])), "UNTITLED")


class PublisherReads:
    def __init__(
        self,
        root: Path,
        authorize: bool,
        max_requests: int = 20,
        seconds: int = 120,
        profile12_proxy: bool = False,
        profile12_port: int = 17890,
    ):
        if not authorize:
            raise AuthorizationRequired("PRC_ANONYMOUS_PUBLIC_READ_AUTHORIZATION_REQUIRED")
        if not 1 <= max_requests <= 40 or not 1 <= seconds <= 300:
            raise ValueError("invalid public acquisition budget")
        if profile12_port not in {17890, 19881}:
            raise ValueError("only verified isolated profile12 listeners are selectable")
        self.root = root
        self.count = 0
        self.limit = max_requests
        self.deadline = time.monotonic() + seconds
        self.route = (
            f"OPERATOR_SELECTED_PROFILE12_LOCAL_{profile12_port}" if profile12_proxy else "DIRECT"
        )
        self.last_receipt: dict = {}
        self.client = httpx.Client(
            trust_env=False,
            follow_redirects=False,
            timeout=8,
            proxy=f"http://127.0.0.1:{profile12_port}" if profile12_proxy else None,
        )

    def close(self):
        self.client.close()

    def get(self, source: str, url: str) -> tuple[bytes, dict]:
        for _ in range(4):
            url = allowed_url(source, url)
            remaining = self.deadline - time.monotonic()
            if self.count >= self.limit or remaining <= 0:
                raise TransportUnknown("PRC_PUBLIC_ACQUISITION_BUDGET_EXHAUSTED")
            self.count += 1
            started = now()
            raw = b""
            status = None
            headers: dict = {}
            error = None
            try:
                self.client.cookies.clear()  # No login/session cookie, including publisher cookies.
                with self.client.stream("GET", url, timeout=min(8, remaining)) as response:
                    status = response.status_code
                    headers = dict(response.headers)
                    chunks = []
                    length = 0
                    for chunk in response.iter_bytes():
                        length += len(chunk)
                        if length > MAX_BYTES or time.monotonic() >= self.deadline:
                            error = "PRC_PUBLIC_RESPONSE_LIMIT_EXCEEDED"
                            break
                        chunks.append(chunk)
                    raw = b"".join(chunks)
            except httpx.HTTPError:
                error = "PRC_PUBLIC_TRANSPORT_FAILED"
            path = self.root / "receipts" / (uuid.uuid4().hex + ".raw")
            private_write(path, raw)
            receipt = {
                "source": source,
                "url": url,
                "method": "GET",
                "started_at": started.isoformat(),
                "received_at": now().isoformat(),
                "http_status": status,
                "content_type": headers.get("content-type"),
                "raw_path": str(path.resolve()),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
                "auth_used": False,
                "error": error,
                "route": self.route,
            }
            self.last_receipt = receipt
            write_json(path.with_suffix(".receipt.json"), receipt)
            if error:
                raise TransportUnknown(error)
            if status in {301, 302, 303, 307, 308}:
                url = urljoin(url, headers.get("location", ""))
                continue  # Validate every hop; never follow a login/off-domain redirect.
            if status != 200:
                raise TransportUnknown("PRC_PUBLIC_HTTP_FAILED")
            return raw, receipt
        raise TransportUnknown("PRC_PUBLIC_REDIRECT_LIMIT_EXCEEDED")


def release_links(source: str, page: Page, base: str) -> list[str]:
    found = []
    for link in page.links:
        url = urljoin(base, link["href"])
        path = urlsplit(url).path
        if source != "QUINNIPIAC" and "2026" not in url + link["label"]:
            continue
        if source == "QUINNIPIAC" and (
            path != "/poll-release"
            or not parse_qs(urlsplit(url).query).get("releaseid")
            or not re.search(r"senate|governor|district", link["label"], re.I)
        ):
            continue
        if source == "MARIST" and not path.startswith("/polls/"):
            continue
        if source == "EMERSON" and (path.count("/") != 2 or path.startswith("/category/")):
            continue
        try:
            url = allowed_url(source, url)
        except SchemaMismatch:
            continue
        if url not in found:
            found.append(url)
    return found


def attachments(source: str, page: Page, base: str) -> tuple[list[str], list[str]]:
    pdfs, external = [], []
    for link in page.links:
        url = urljoin(base, link["href"])
        label = link["label"].casefold()
        if not (
            urlsplit(url).path.lower().endswith(".pdf")
            or re.search("survey data|full results", label)
        ):
            continue
        try:
            url = allowed_url(source, url)
            if urlsplit(url).path.lower().endswith(".pdf") and url not in pdfs:
                pdfs.append(url)
        except SchemaMismatch:
            # Record the exact advertised link; do not invent export/private endpoints.
            if urlsplit(url).scheme == "https" and url not in external:
                external.append(url)
    return pdfs, external


def extract_pdf(raw_path: str) -> tuple[str | None, str]:
    executable = shutil.which("pdftotext")
    if not executable:
        return None, "PDF_TEXT_TOOL_ABSENT"
    try:
        result = subprocess.run(
            [executable, "-layout", raw_path, "-"], capture_output=True, timeout=10, env={}
        )
    except subprocess.TimeoutExpired:
        return None, "PDF_TEXT_EXTRACTION_TIMEOUT"
    if result.returncode:
        return None, "PDF_TEXT_EXTRACTION_FAILED"
    return result.stdout.decode("utf-8", errors="strict"), "EXTRACTED_UNREVIEWED"


def collect(
    root: Path = ROOT,
    *,
    authorize: bool,
    per_source: int = 3,
    max_requests: int = 20,
    seconds: int = 120,
    profile12_proxy: bool = False,
    profile12_port: int = 17890,
):
    if not authorize:
        raise AuthorizationRequired("PRC_ANONYMOUS_PUBLIC_READ_AUTHORIZATION_REQUIRED")
    if not 1 <= per_source <= 10:
        raise ValueError("per-source releases must be 1..10")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (root / "acquisition.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise AuthorizationRequired("PRC_PUBLIC_ACQUISITION_ALREADY_RUNNING") from None
        client = PublisherReads(
            root, authorize, max_requests, seconds, profile12_proxy, profile12_port
        )
        documents, failures, discovery = [], [], []
        index_path = root / "index.json"
        old = json.loads(index_path.read_text()) if index_path.exists() else {"documents": []}
        known = {d["document_id"]: d for d in old["documents"]}
        try:
            for source, index_url in SOURCES.items():
                try:
                    raw, index_receipt = client.get(source, index_url)
                    page = Page(raw.decode("utf-8"))
                    links = release_links(source, page, index_receipt["url"])
                    if not links:
                        failures.append(
                            {
                                "source": source,
                                "stage": "INDEX",
                                "code": "PRC_NO_RECOGNIZED_RELEASE_LINKS",
                            }
                        )
                    discovery.append(
                        {
                            "source": source,
                            "index_url": index_url,
                            "linked_releases": len(links),
                            "selected": min(len(links), per_source),
                            "full_history_complete": False,
                        }
                    )
                except (TransportUnknown, UnicodeError) as exc:
                    failures.append(
                        {
                            "source": source,
                            "stage": "INDEX",
                            "code": exc.code
                            if isinstance(exc, TransportUnknown)
                            else "HTML_ENCODING_INVALID",
                        }
                    )
                    continue
                for url in links[:per_source]:
                    try:
                        raw, receipt = client.get(source, url)
                        page = Page(raw.decode("utf-8"))
                        text = "\n".join(page.text)
                        pdfs, external = attachments(source, page, receipt["url"])
                        doc_id = hashlib.sha256((source + ":" + url).encode()).hexdigest()[:24]
                        text_path = root / "text" / (receipt["raw_sha256"] + ".txt")
                        preserve_text(text_path, text)
                        pdf_documents = []
                        for pdf in pdfs[:2]:
                            try:
                                pdf_raw, pdf_receipt = client.get(source, pdf)
                                if not pdf_raw.startswith(b"%PDF-"):
                                    raise SchemaMismatch("PRC_ADVERTISED_PDF_INVALID")
                                pdf_text, extraction = extract_pdf(pdf_receipt["raw_path"])
                                pdf_receipt["text_status"] = extraction
                                if pdf_text:
                                    p = root / "text" / (pdf_receipt["raw_sha256"] + ".txt")
                                    preserve_text(p, pdf_text)
                                    pdf_receipt["text_path"] = str(p.resolve())
                                    pdf_receipt["text_sha256"] = hashlib.sha256(
                                        pdf_text.encode()
                                    ).hexdigest()
                                pdf_documents.append(pdf_receipt)
                            except (TransportUnknown, SchemaMismatch) as exc:
                                failures.append(
                                    {"source": source, "stage": "PDF", "code": exc.code}
                                )
                        previous = known.get(doc_id)
                        document = {
                            "document_id": doc_id,
                            "organization": source,
                            "title": page.title,
                            "source_url": url,
                            "receipt": receipt,
                            "first_received_at": previous["first_received_at"]
                            if previous
                            else receipt["received_at"],
                            "available_at": now().isoformat(),
                            "text_path": str(text_path.resolve()),
                            "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
                            "publication_claims": page.meta.get("article:published_time", [])
                            + page.meta.get("datePublished", []),
                            "first_public_at": None,
                            "first_public_verified": False,
                            "pdf_documents": pdf_documents,
                            "external_table_links": external,
                            "review_status": "UNREVIEWED_DOCUMENT_NOT_POLL_INPUT",
                            "report_questions": 0,
                            "event_mapping_approved": False,
                            "contrast_variance": None,
                            "authorized_for_live": False,
                            "receipt_history": [
                                *(previous or {}).get("receipt_history", []),
                                receipt,
                            ],
                        }
                        if previous and previous["receipt"]["raw_sha256"] != receipt["raw_sha256"]:
                            document["prior_versions"] = [
                                *previous.get("prior_versions", []),
                                {
                                    k: v
                                    for k, v in previous.items()
                                    if k not in {"prior_versions", "receipt_history"}
                                },
                            ]
                        elif previous:
                            document["prior_versions"] = previous.get("prior_versions", [])
                            # An unchanged document keeps its earliest usable evidence.
                            document["receipt"] = previous["receipt"]
                            document["available_at"] = previous["available_at"]
                            document["pdf_documents"] = previous["pdf_documents"] or pdf_documents
                        known[doc_id] = document
                        documents.append(document)
                    except (TransportUnknown, UnicodeError) as exc:
                        failures.append(
                            {
                                "source": source,
                                "stage": "RELEASE",
                                "code": exc.code
                                if isinstance(exc, TransportUnknown)
                                else "HTML_ENCODING_INVALID",
                            }
                        )
            write_json(
                index_path, {"updated_at": now().isoformat(), "documents": list(known.values())}
            )
        finally:
            client.close()
        result = {
            "layer": "REAL_READ",
            "mode": "ANONYMOUS_PUBLIC_READ",
            "strategy_id": "prc_sig",
            "status": "PARTIAL" if failures else "PASS_BOUNDED_DOCUMENT_ACQUISITION_ONLY",
            "observed_at": now().isoformat(),
            "network_attempts": client.count,
            "credentials_loaded": False,
            "route": client.route,
            "real_orders": 0,
            "real_writes": 0,
            "discovery": discovery,
            "failures": failures,
            "release_documents_this_run": len(documents),
            "archived_release_documents": len(known),
            "pdf_documents_this_run": sum(len(d["pdf_documents"]) for d in documents),
            "extracted_pdfs_this_run": sum(
                bool(p.get("text_path")) for d in documents for p in d["pdf_documents"]
            ),
            "external_table_links_pending": sum(len(d["external_table_links"]) for d in documents),
            "normalization": "NOT_PERFORMED_DOCUMENT_ACQUISITION_ONLY",
            "candidate_evaluation": "NOT_RUN_DOCUMENTS_ARE_UNREVIEWED",
            "qualified_candidates": 0,
            "full_history_complete": False,
            "documents": [
                {
                    "document_id": d["document_id"],
                    "organization": d["organization"],
                    "title": d["title"],
                    "source_url": d["source_url"],
                    "raw_sha256": d["receipt"]["raw_sha256"],
                    "publication_claims": d["publication_claims"],
                    "first_public_verified": False,
                    "pdfs": len(d["pdf_documents"]),
                }
                for d in documents
            ],
            "remaining": [
                "QUESTION_EVENT_MAPPING_REVIEW",
                "FIRST_PUBLIC_TIME_AND_60_SECOND_RECEIPT",
                "COMPARABLE_PAIR_AND_CONTRAST_VARIANCE",
                "SIG_WINDOW_RULE_FEE_ACCOUNT_GATES",
            ],
        }
        write_json(root / "last-run.json", result)
        return result
