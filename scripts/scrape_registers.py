"""Scrape register documents for councillors and store them in PostgreSQL."""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Iterable, Optional

import pdfplumber
import requests
from bs4 import BeautifulSoup

from config import get_db_connection
from parsers.council_parsers import (
    crawl_council_register_pages,
    find_councillor_index_pages,
    find_councillor_links,
    find_council_homepage,
    find_pdf_links,
    find_register_links,
    find_register_pages_for_councillor,
    find_ward_link,
)

logger = logging.getLogger(__name__)
_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}
_REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0"))


def fetch_councillors() -> Iterable[tuple[int, str, str, Optional[str]]]:
    """Yield councillor rows from the database."""

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, council, ward
                FROM councillors
                ORDER BY id
                """
            )
            rows = cur.fetchall()

    for row in rows:
        yield row[0], row[1], row[2], row[3]


def log_audit(
    councillor_id: Optional[int],
    issue_type: str,
    details: Optional[str],
) -> None:
    """Insert a scraping audit entry for missing data or failures."""

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scraping_audit (councillor_id, issue_type, details)
                VALUES (%s, %s, %s)
                """,
                (councillor_id, issue_type, details),
            )


def store_register(
    councillor_id: int,
    register_url: str,
    content_type: str,
    pdf_bytes: Optional[bytes],
    extracted_text: Optional[str],
) -> None:
    """Persist a register document and extracted text to the database."""

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO councillor_registers (
                    councillor_id,
                    register_url,
                    content_type,
                    pdf_bytes,
                    extracted_text
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (councillor_id, register_url, content_type, pdf_bytes, extracted_text),
            )


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from a PDF byte string using pdfplumber."""

    chunks: list[str] = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                if text:
                    chunks.append(text)
    except Exception as exc:  # noqa: BLE001 - treat invalid PDFs as empty text.
        logger.warning("PDF extraction failed: %s", exc)
        return ""

    return "\n\n".join(chunks).strip()


def fetch_register_content(register_url: str) -> tuple[str, bytes, str]:
    """Download the register URL and return (content_type, bytes, text)."""

    if _REQUEST_DELAY:
        time.sleep(_REQUEST_DELAY)
    response = requests.get(register_url, headers=_REQUEST_HEADERS, timeout=30)
    response.raise_for_status()

    content_type = (response.headers.get("Content-Type") or "").lower()
    if not content_type:
        content_type = (
            "application/pdf" if register_url.lower().endswith(".pdf") else "text/html"
        )

    return content_type, response.content, response.text


def _name_matches(text: str, name: str) -> bool:
    """Return True when councillor name appears in extracted text."""

    if not text:
        return False
    lowered = text.lower()
    target = name.lower().strip()
    if target in lowered:
        return True

    name_parts = [part for part in re.split(r"[^a-z]+", target) if part]
    if len(name_parts) < 2:
        return False

    first = name_parts[0]
    surname = name_parts[-1]
    tokens = [t for t in re.split(r"[^a-z]+", lowered) if t]
    if not tokens:
        return False

    for i, token in enumerate(tokens):
        if token != surname:
            continue
        window = tokens[max(0, i - 3) : i + 4]
        if any(w == first or w.startswith(first[0]) for w in window):
            return True

    for i, token in enumerate(tokens):
        if token != surname:
            continue
        window = tokens[i + 1 : i + 4]
        if any(w == first or w.startswith(first[0]) for w in window):
            return True

    return False


def _looks_like_register_text(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    phrases = [
        "register of interests",
        "register of member interests",
        "register of members interests",
        "members' interests",
        "member's interests",
        "declaration of interest",
        "declarations of interest",
        "pecuniary interests",
        "disclosable pecuniary interests",
    ]
    return any(phrase in lowered for phrase in phrases)


def _looks_like_register_url(url: str) -> bool:
    lowered = url.lower()
    return any(
        hint in lowered
        for hint in (
            "mgdeclarationsubmission",
            "mgrofi",
            "registerofinterests",
            "register-of-interests",
            "register-of-members-interests",
        )
    )


def _looks_like_register_url(url: str) -> bool:
    lowered = url.lower()
    return any(
        hint in lowered
        for hint in (
            "mgdeclarationsubmission",
            "mgrofi",
            "registerofinterests",
            "register-of-interests",
            "register-of-members-interests",
        )
    )


def _fetch_and_extract(
    register_url: str,
    register_content_cache: dict[str, tuple[str, Optional[bytes], str]],
    cache_lock: Lock,
) -> Optional[tuple[str, Optional[bytes], str]]:
    """Fetch a URL, extract text, and cache the results."""

    with cache_lock:
        cached = register_content_cache.get(register_url)
    if cached:
        return cached

    content_type, raw_bytes, raw_text = fetch_register_content(register_url)
    is_pdf = "pdf" in content_type or register_url.lower().endswith(".pdf")
    if is_pdf:
        extracted_text = ""
        pdf_bytes = None
    else:
        soup = BeautifulSoup(raw_text, "html.parser")
        extracted_text = soup.get_text(" ", strip=True)
        pdf_bytes = None

    with cache_lock:
        register_content_cache[register_url] = (content_type, pdf_bytes, extracted_text)
    return register_content_cache[register_url]


def get_cached_homepage(council: str) -> Optional[str]:
    """Return cached council homepage if present."""

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT homepage_url FROM council_homepages WHERE council = %s",
                (council,),
            )
            row = cur.fetchone()
            return row[0] if row else None


def cache_homepage(council: str, homepage_url: str) -> None:
    """Upsert a council homepage URL into the cache table."""

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO council_homepages (council, homepage_url)
                VALUES (%s, %s)
                ON CONFLICT (council) DO UPDATE
                SET homepage_url = EXCLUDED.homepage_url,
                    discovered_at = NOW()
                """,
                (council, homepage_url),
            )


def councillor_has_match(councillor_id: int) -> bool:
    """Return True if this councillor already has a stored register."""

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM councillor_registers WHERE councillor_id = %s LIMIT 1",
                (councillor_id,),
            )
            return cur.fetchone() is not None


def _process_councillor(
    councillor_id: int,
    name: str,
    council: str,
    ward: Optional[str],
    totals: dict[str, int],
    totals_lock: Lock,
    missing_rows: list[tuple[int, str, str, Optional[str]]],
    missing_lock: Lock,
    link_rows: list[tuple[str, Optional[str], str, str]],
    link_lock: Lock,
    pdf_rows: list[tuple[str, str, str]],
    pdf_lock: Lock,
    register_content_cache: dict[str, tuple[str, Optional[bytes], str]],
    cache_lock: Lock,
    failure_rows: list[tuple[str, str, str]],
    failure_lock: Lock,
    index_page_cache: dict[str, list[str]],
    index_lock: Lock,
    flow_counts: dict[str, int],
    flow_lock: Lock,
) -> None:
    if councillor_has_match(councillor_id):
        with totals_lock:
            totals["processed"] += 1
        logger.info("Skipping %s (already has match)", name)
        return

    with totals_lock:
        totals["processed"] += 1

    homepage = get_cached_homepage(council)
    if not homepage:
        homepage = find_council_homepage(council)
        if homepage:
            cache_homepage(council, homepage)
    if homepage:
        logger.info("Council homepage for %s: %s", council, homepage)

    homepage_html = ""
    if homepage:
        try:
            response = requests.get(homepage, headers=_REQUEST_HEADERS, timeout=30)
            response.raise_for_status()
            homepage_html = response.text
        except Exception as exc:  # noqa: BLE001 - best-effort only.
            logger.debug("Failed to fetch homepage HTML for %s: %s", council, exc)

    ward_url = ""
    if homepage_html and ward:
        ward_link = find_ward_link(homepage, homepage_html, ward)
        if ward_link:
            ward_url = ward_link

    logger.info(
        "Processing %s (%s, %s)",
        name,
        council,
        ward or "no ward",
    )

    register_pages: list[str] = []
    councillor_page_url = ""
    flow_path = "fallback_search"

    index_pages: list[str] = []
    if homepage:
        with index_lock:
            index_pages = index_page_cache.get(council, [])
        if not index_pages:
            index_pages = find_councillor_index_pages(council, homepage)
            with index_lock:
                index_page_cache[council] = index_pages

    for index_url in index_pages:
        try:
            response = requests.get(index_url, headers=_REQUEST_HEADERS, timeout=30)
            response.raise_for_status()
        except Exception:
            continue

        links = find_councillor_links(index_url, response.text, name)
        if links:
            councillor_page_url = links[0]
            break

    if councillor_page_url:
        try:
            response = requests.get(
                councillor_page_url, headers=_REQUEST_HEADERS, timeout=30
            )
            response.raise_for_status()
            register_pages.extend(find_register_links(councillor_page_url, response.text))
            register_pages.extend(find_pdf_links(councillor_page_url, response.text))
            if register_pages:
                flow_path = "councillor_page"
        except Exception:
            pass

    if not register_pages:
        try:
            register_pages = find_register_pages_for_councillor(name, council, ward)
        except Exception as exc:  # noqa: BLE001 - report errors without crashing the loop.
            with totals_lock:
                totals["search_error"] += 1
            with failure_lock:
                failure_rows.append((name, council, "search_error"))
            log_audit(
                councillor_id,
                "search_error",
                f"Search failed: {exc}",
            )
            logger.warning("Search failed for %s: %s", name, exc)
            return

    if not register_pages:
        try:
            register_pages = crawl_council_register_pages(council, homepage=homepage)
        except Exception as exc:  # noqa: BLE001 - best-effort fallback.
            logger.debug("Council crawl fallback failed for %s: %s", council, exc)

    matched = False
    councillor_page_url = ""
    for register_url in register_pages:
        try:
            fetched = _fetch_and_extract(register_url, register_content_cache, cache_lock)
        except Exception as exc:  # noqa: BLE001
            with totals_lock:
                totals["register_fetch_error"] += 1
            log_audit(
                councillor_id,
                "register_fetch_error",
                f"Failed to download register: {exc}",
            )
            logger.warning(
                "Register fetch error for %s (%s): %s", name, register_url, exc
            )
            continue

        if not fetched:
            continue
        content_type, pdf_bytes, extracted_text = fetched

        if content_type.startswith("application/pdf") or register_url.lower().endswith(".pdf"):
            with pdf_lock:
                pdf_rows.append((name, council, register_url))

        if not _name_matches(extracted_text, name):
            if content_type.startswith("text/html"):
                try:
                    response = requests.get(
                        register_url, headers=_REQUEST_HEADERS, timeout=30
                    )
                    response.raise_for_status()
                except Exception as exc:  # noqa: BLE001 - best-effort only.
                    logger.debug(
                        "Failed to re-fetch HTML for %s: %s", register_url, exc
                    )
                    continue

                    candidate_links = find_councillor_links(
                        register_url, response.text, name
                    )[:5]
                    if _REQUEST_DELAY:
                        time.sleep(_REQUEST_DELAY)
                for candidate_url in candidate_links:
                    try:
                        candidate_fetched = _fetch_and_extract(
                            candidate_url, register_content_cache, cache_lock
                        )
                    except Exception as exc:  # noqa: BLE001
                        with totals_lock:
                            totals["register_fetch_error"] += 1
                        log_audit(
                            councillor_id,
                            "register_fetch_error",
                            f"Failed to download councillor link: {exc}",
                        )
                        logger.warning(
                            "Councillor link fetch error for %s (%s): %s",
                            name,
                            candidate_url,
                            exc,
                        )
                        continue

                    if not candidate_fetched:
                        continue
                    link_content_type, link_pdf_bytes, link_text = candidate_fetched
                    if not _name_matches(link_text, name):
                        continue
                    if not (
                        _looks_like_register_text(link_text)
                        or _looks_like_register_url(candidate_url)
                    ):
                        continue

                    store_register(
                        councillor_id,
                        candidate_url,
                        link_content_type,
                        link_pdf_bytes,
                        link_text,
                    )
                    with totals_lock:
                        totals["stored"] += 1
                    matched = True
                    councillor_page_url = candidate_url
                    break

                if not matched:
                    pdf_links = find_pdf_links(register_url, response.text)[:10]
                    for pdf_url in pdf_links:
                        with pdf_lock:
                            pdf_rows.append((name, council, pdf_url))

                if matched:
                    break
            continue

        if _looks_like_register_text(extracted_text) or _looks_like_register_url(
            register_url
        ):
            store_register(
                councillor_id,
                register_url,
                content_type,
                pdf_bytes,
                extracted_text,
            )
            with totals_lock:
                totals["stored"] += 1
            matched = True
            councillor_page_url = register_url
            break

    if not matched:
        with totals_lock:
            totals["missing_register_url"] += 1
        with missing_lock:
            missing_rows.append((councillor_id, name, council, ward))
            log_audit(
                councillor_id,
                "missing_register_url",
                "No register of interests page contained the councillor name.",
            )
        with failure_lock:
            failure_rows.append((name, council, "missing_register_url"))

    with link_lock:
        link_rows.append((name, ward, ward_url, councillor_page_url))
    with flow_lock:
        flow_counts[flow_path] = flow_counts.get(flow_path, 0) + 1


def scrape_registers() -> None:
    """Iterate councillors, download registers, and store results."""

    totals = {
        "processed": 0,
        "missing_register_url": 0,
        "register_fetch_error": 0,
        "search_error": 0,
        "stored": 0,
    }
    totals_lock = Lock()
    missing_rows: list[tuple[int, str, str, Optional[str]]] = []
    missing_lock = Lock()
    link_rows: list[tuple[str, Optional[str], str, str]] = []
    link_lock = Lock()
    pdf_rows: list[tuple[str, str, str]] = []
    pdf_lock = Lock()
    failure_rows: list[tuple[str, str, str]] = []
    failure_lock = Lock()
    index_page_cache: dict[str, list[str]] = {}
    index_lock = Lock()
    flow_counts: dict[str, int] = {}
    flow_lock = Lock()

    register_content_cache: dict[str, tuple[str, Optional[bytes], str]] = {}
    cache_lock = Lock()

    max_workers = int(os.getenv("SCRAPER_WORKERS", "6"))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for councillor_id, name, council, ward in fetch_councillors():
            futures.append(
                executor.submit(
                    _process_councillor,
                    councillor_id,
                    name,
                    council,
                    ward,
                    totals,
                    totals_lock,
                    missing_rows,
                    missing_lock,
                    link_rows,
                    link_lock,
                    pdf_rows,
                    pdf_lock,
                    register_content_cache,
                    cache_lock,
                    failure_rows,
                    failure_lock,
                    index_page_cache,
                    index_lock,
                    flow_counts,
                    flow_lock,
                )
            )
        for future in as_completed(futures):
            _ = future.result()

    if missing_rows:
        missing_path = "missing_councillors.csv"
        with open(missing_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "name", "council", "ward"])
            for row in missing_rows:
                writer.writerow(row)
        logger.info("Wrote missing councillors report to %s", missing_path)

    if link_rows:
        links_path = "councillor_links.csv"
        with open(links_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["name", "ward", "ward_url", "councillor_page_url"])
            for row in link_rows:
                writer.writerow(row)
        logger.info("Wrote councillor links report to %s", links_path)

    if pdf_rows:
        pdf_path = "manual_pdf_registers.csv"
        with open(pdf_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["name", "council", "pdf_url"])
            for row in pdf_rows:
                writer.writerow(row)
        logger.info("Wrote manual PDF register list to %s", pdf_path)
        logger.info("Manual PDF register entries: %s", len(pdf_rows))
    else:
        logger.info("Manual PDF register entries: 0")

    if failure_rows:
        failure_path = "failed_councillors.csv"
        with open(failure_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["name", "council", "reason"])
            for row in failure_rows:
                writer.writerow(row)
        logger.info("Wrote failure summary to %s", failure_path)
        counts: dict[str, int] = {}
        for _name, _council, reason in failure_rows:
            counts[reason] = counts.get(reason, 0) + 1
        logger.info("Failure counts by reason: %s", counts)

    logger.info("Flow counts: %s", flow_counts)

    logger.info(
        "Finished. processed=%s stored=%s missing_register_url=%s "
        "register_fetch_error=%s search_error=%s",
        totals["processed"],
        totals["stored"],
        totals["missing_register_url"],
        totals["register_fetch_error"],
        totals["search_error"],
    )


if __name__ == "__main__":
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")
    scrape_registers()
