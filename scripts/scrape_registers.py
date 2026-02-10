"""Scrape register documents for councillors and store them in PostgreSQL."""

from __future__ import annotations

import csv
import io
import logging
import os
import re
from typing import Iterable, Optional

import pdfplumber
import requests
from bs4 import BeautifulSoup

from config import get_db_connection
from parsers.council_parsers import (
    crawl_council_register_pages,
    find_councillor_links,
    find_council_homepage,
    find_pdf_links,
)

logger = logging.getLogger(__name__)
_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}


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
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if text:
                chunks.append(text)

    return "\n\n".join(chunks).strip()


def fetch_register_content(register_url: str) -> tuple[str, bytes, str]:
    """Download the register URL and return (content_type, bytes, text)."""

    response = requests.get(register_url, headers=_REQUEST_HEADERS, timeout=30)
    response.raise_for_status()

    content_type = (response.headers.get("Content-Type") or "").lower()
    if not content_type:
        content_type = "application/pdf" if register_url.lower().endswith(".pdf") else "text/html"

    return content_type, response.content, response.text


def _name_matches(text: str, name: str) -> bool:
    """Return True when councillor name appears in extracted text."""

    if not text:
        return False
    lowered = text.lower()
    target = name.lower().strip()
    if target in lowered:
        return True

    # Fuzzy match: allow initials and surname order.
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
        window = tokens[max(0, i - 3): i + 4]
        if any(w == first or w.startswith(first[0]) for w in window):
            return True

    # Handle "Surname, First" formats.
    for i, token in enumerate(tokens):
        if token != surname:
            continue
        window = tokens[i + 1: i + 4]
        if any(w == first or w.startswith(first[0]) for w in window):
            return True

    return False


def _fetch_and_extract(
    register_url: str,
    register_content_cache: dict[str, tuple[str, Optional[bytes], str]],
) -> Optional[tuple[str, Optional[bytes], str]]:
    """Fetch a URL, extract text, and cache the results."""

    cached = register_content_cache.get(register_url)
    if cached:
        return cached

    content_type, raw_bytes, raw_text = fetch_register_content(register_url)
    is_pdf = "pdf" in content_type or register_url.lower().endswith(".pdf")
    if is_pdf:
        extracted_text = extract_pdf_text(raw_bytes)
        pdf_bytes = raw_bytes
    else:
        soup = BeautifulSoup(raw_text, "html.parser")
        extracted_text = soup.get_text(" ", strip=True)
        pdf_bytes = None

    register_content_cache[register_url] = (content_type, pdf_bytes, extracted_text)
    return register_content_cache[register_url]


def scrape_registers() -> None:
    """Iterate councillors, download registers, and store results."""

    totals = {
        "processed": 0,
        "missing_register_url": 0,
        "register_fetch_error": 0,
        "search_error": 0,
        "stored": 0,
    }

    council_register_cache: dict[str, list[str]] = {}
    register_content_cache: dict[str, tuple[str, Optional[bytes], str]] = {}

    missing_rows: list[tuple[int, str, str, Optional[str]]] = []

    for councillor_id, name, council, ward in fetch_councillors():
        totals["processed"] += 1
        logger.info(
            "Processing %s (%s, %s)",
            name,
            council,
            ward or "no ward",
        )

        register_pages = council_register_cache.get(council)
        if register_pages is None:
            logger.info("Crawling council site for %s", council)
            cached_homepage = get_cached_homepage(council)
            homepage = cached_homepage
            if not homepage:
                homepage = find_council_homepage(council)
                if homepage:
                    cache_homepage(council, homepage)

            try:
                register_pages = crawl_council_register_pages(council, homepage=homepage)
            except Exception as exc:  # noqa: BLE001 - report errors without crashing the loop.
                totals["search_error"] += 1
                log_audit(
                    councillor_id,
                    "search_error",
                    f"Council crawl failed: {exc}",
                )
                logger.warning("Council crawl failed for %s: %s", council, exc)
                continue

            council_register_cache[council] = register_pages
            logger.info(
                "Found %s register page(s) for %s",
                len(register_pages),
                council,
            )
            if register_pages:
                logger.info("Register pages for %s: %s", council, register_pages)

        matched = False
        for register_url in register_pages:
            cached = register_content_cache.get(register_url)
            if cached:
                content_type, pdf_bytes, extracted_text = cached
                logger.debug(
                    "Cache hit for %s content_type=%s text_len=%s",
                    register_url,
                    content_type,
                    len(extracted_text or ""),
                )
            else:
                try:
                    content_type, raw_bytes, raw_text = fetch_register_content(register_url)
                except Exception as exc:  # noqa: BLE001 - keep going on fetch failures.
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

                is_pdf = "pdf" in content_type or register_url.lower().endswith(".pdf")
                if is_pdf:
                    extracted_text = extract_pdf_text(raw_bytes)
                    pdf_bytes = raw_bytes
                else:
                    soup = BeautifulSoup(raw_text, "html.parser")
                    extracted_text = soup.get_text(" ", strip=True)
                    pdf_bytes = None

                register_content_cache[register_url] = (
                    content_type,
                    pdf_bytes,
                    extracted_text,
                )
                logger.info(
                    "Fetched %s content_type=%s text_len=%s",
                    register_url,
                    content_type,
                    len(extracted_text or ""),
                )

            if not _name_matches(extracted_text, name):
                # If this is HTML, try to follow councillor-specific links.
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
                    logger.info(
                        "Found %s councillor link(s) on %s for %s",
                        len(candidate_links),
                        register_url,
                        name,
                    )
                    for candidate_url in candidate_links:
                        try:
                            fetched = _fetch_and_extract(
                                candidate_url, register_content_cache
                            )
                        except Exception as exc:  # noqa: BLE001 - keep going on failures.
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

                        if not fetched:
                            continue
                        link_content_type, link_pdf_bytes, link_text = fetched
                        if not _name_matches(link_text, name):
                            continue

                        store_register(
                            councillor_id,
                            candidate_url,
                            link_content_type,
                            link_pdf_bytes,
                            link_text,
                        )
                        totals["stored"] += 1
                        matched = True
                        logger.info(
                            "Stored register for %s (%s)", name, candidate_url
                        )
                        break

                    if not matched:
                        pdf_links = find_pdf_links(register_url, response.text)[:10]
                        logger.info(
                            "Found %s PDF link(s) on %s for %s",
                            len(pdf_links),
                            register_url,
                            name,
                        )
                        for pdf_url in pdf_links:
                            try:
                                fetched = _fetch_and_extract(
                                    pdf_url, register_content_cache
                                )
                            except Exception as exc:  # noqa: BLE001 - keep going on failures.
                                totals["register_fetch_error"] += 1
                                log_audit(
                                    councillor_id,
                                    "register_fetch_error",
                                    f"Failed to download register PDF: {exc}",
                                )
                                logger.warning(
                                    "Register PDF fetch error for %s (%s): %s",
                                    name,
                                    pdf_url,
                                    exc,
                                )
                                continue

                            if not fetched:
                                continue
                            pdf_content_type, pdf_bytes, pdf_text = fetched
                            if not _name_matches(pdf_text, name):
                                continue

                            store_register(
                                councillor_id,
                                pdf_url,
                                pdf_content_type,
                                pdf_bytes,
                                pdf_text,
                            )
                            totals["stored"] += 1
                            matched = True
                            logger.info("Stored register for %s (%s)", name, pdf_url)
                            break

                    if matched:
                        break
                continue

            store_register(
                councillor_id,
                register_url,
                content_type,
                pdf_bytes,
                extracted_text,
            )
            totals["stored"] += 1
            matched = True
            logger.info("Stored register for %s (%s)", name, register_url)
            break

        if not matched:
            totals["missing_register_url"] += 1
            missing_rows.append((councillor_id, name, council, ward))
            log_audit(
                councillor_id,
                "missing_register_url",
                "No register of interests page contained the councillor name.",
            )
            logger.info("No register page matched for %s", name)

    if missing_rows:
        missing_path = "missing_councillors.csv"
        with open(missing_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "name", "council", "ward"])
            for row in missing_rows:
                writer.writerow(row)
        logger.info("Wrote missing councillors report to %s", missing_path)

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
