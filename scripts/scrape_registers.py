"""Scrape register documents for councillors and store them in PostgreSQL."""

from __future__ import annotations

import io
from typing import Iterable, Optional

import pdfplumber
import requests
from bs4 import BeautifulSoup

from config import get_db_connection
from parsers.council_parsers import find_register_url_generic


def fetch_councillors() -> Iterable[tuple[int, str, Optional[str]]]:
    """Yield councillor rows from the database."""

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, profile_url FROM councillors ORDER BY id")
            rows = cur.fetchall()

    for row in rows:
        yield row[0], row[1], row[2]


def log_audit(
    councillor_id: Optional[int],
    profile_url: Optional[str],
    issue_type: str,
    details: Optional[str],
) -> None:
    """Insert a scraping audit entry for missing data or failures."""

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scraping_audit (councillor_id, profile_url, issue_type, details)
                VALUES (%s, %s, %s, %s)
                """,
                (councillor_id, profile_url, issue_type, details),
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


def scrape_registers() -> None:
    """Iterate councillors, download registers, and store results."""

    for councillor_id, name, profile_url in fetch_councillors():
        if not profile_url:
            log_audit(
                councillor_id,
                profile_url,
                "missing_profile_url",
                f"No profile URL for councillor: {name}",
            )
            continue

        try:
            register_url = find_register_url_generic(profile_url)
        except Exception as exc:  # noqa: BLE001 - report errors without crashing the loop.
            log_audit(
                councillor_id,
                profile_url,
                "profile_fetch_error",
                f"Failed to parse profile page: {exc}",
            )
            continue

        if not register_url:
            log_audit(
                councillor_id,
                profile_url,
                "missing_register_url",
                "No register of interests link found on profile page.",
            )
            continue

        try:
            content_type, raw_bytes, raw_text = fetch_register_content(register_url)
        except Exception as exc:  # noqa: BLE001 - report fetch errors without crashing the loop.
            log_audit(
                councillor_id,
                profile_url,
                "register_fetch_error",
                f"Failed to download register: {exc}",
            )
            continue

        is_pdf = "pdf" in content_type or register_url.lower().endswith(".pdf")
        if is_pdf:
            extracted_text = extract_pdf_text(raw_bytes)
            pdf_bytes = raw_bytes
        else:
            # Parse HTML content into plain text for storage.
            soup = BeautifulSoup(raw_text, "html.parser")
            extracted_text = soup.get_text(" ", strip=True)
            pdf_bytes = None

        store_register(
            councillor_id,
            register_url,
            content_type,
            pdf_bytes,
            extracted_text,
        )


if __name__ == "__main__":
    scrape_registers()
