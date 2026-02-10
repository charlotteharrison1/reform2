"""Scrape register documents for councillors and store them in PostgreSQL."""

from __future__ import annotations

import io
import logging
import os
from typing import Iterable, Optional

import pdfplumber
import requests
from bs4 import BeautifulSoup

from config import get_db_connection
from parsers.council_parsers import (
    find_councillor_links,
    find_register_pages_for_councillor,
)

logger = logging.getLogger(__name__)

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
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if text:
                chunks.append(text)

    return "\n\n".join(chunks).strip()


def fetch_register_content(register_url: str) -> tuple[str, bytes, str]:
    """Download the register URL and return (content_type, bytes, text)."""

    response = requests.get(register_url, timeout=30)
    response.raise_for_status()

    content_type = (response.headers.get("Content-Type") or "").lower()
    if not content_type:
        content_type = "application/pdf" if register_url.lower().endswith(".pdf") else "text/html"

    return content_type, response.content, response.text


def _name_matches(text: str, name: str) -> bool:
    """Return True when councillor name appears in extracted text."""

    if not text:
        return False
    return name.lower() in text.lower()


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

    register_content_cache: dict[str, tuple[str, Optional[bytes], str]] = {}

    for councillor_id, name, council, ward in fetch_councillors():
        totals["processed"] += 1
        logger.info(
            "Processing %s (%s, %s)",
            name,
            council,
            ward or "no ward",
        )

        logger.info("Searching register pages for %s", name)
        try:
            register_pages = find_register_pages_for_councillor(name, council, ward)
        except Exception as exc:  # noqa: BLE001 - report errors without crashing the loop.
            totals["search_error"] += 1
            log_audit(
                councillor_id,
                "search_error",
                f"Search failed: {exc}",
            )
            logger.warning("Search failed for %s: %s", name, exc)
            continue

        logger.info(
            "Found %s register page(s) for %s", len(register_pages), name
        )
        if register_pages:
            logger.info("Register pages for %s: %s", name, register_pages)

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
                        response = requests.get(register_url, timeout=30)
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
            log_audit(
                councillor_id,
                "missing_register_url",
                "No register of interests page contained the councillor name.",
            )
            logger.info("No register page matched for %s", name)

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
