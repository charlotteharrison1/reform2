"""Fetch register-of-interests pages and extract searchable text."""

from __future__ import annotations

import csv
import os
import time
from typing import Iterable
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

INPUT = os.getenv("REFORM_COUNCILLORS_CSV", "reform_councillor_pages.csv")
OUTPUT = os.getenv("REGISTER_TEXTS_CSV", "reform_register_texts_clean.csv")
PDF_OUTPUT = os.getenv("REGISTER_PDF_CSV", "reform_register_pdfs.csv")
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}


def _log(msg: str) -> None:
    if LOG_LEVEL in {"INFO", "DEBUG"}:
        print(msg)


def _split_register_urls(value: str) -> Iterable[str]:
    if not value:
        return []
    parts = [p.strip() for p in value.split("|")]
    return [p for p in parts if p]


def _looks_like_pdf(url: str, content_type: str) -> bool:
    if "pdf" in (content_type or "").lower():
        return True
    return url.lower().endswith(".pdf")


def _extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(" ", strip=True)


def main() -> None:
    rows = []
    with open(INPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            register_url = (row.get("register_url") or "").strip()
            if not register_url:
                continue
            rows.append(
                (
                    (row.get("council") or "").strip(),
                    (row.get("councillor") or "").strip(),
                    (row.get("ward") or "").strip(),
                    register_url,
                )
            )

    existing_texts = set()
    if os.path.exists(OUTPUT):
        with open(OUTPUT, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (
                    (row.get("council") or "").strip().lower(),
                    (row.get("councillor") or "").strip().lower(),
                    (row.get("register_url") or "").strip().lower(),
                )
                if all(key):
                    existing_texts.add(key)

    existing_pdfs = set()
    if os.path.exists(PDF_OUTPUT):
        with open(PDF_OUTPUT, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (
                    (row.get("council") or "").strip().lower(),
                    (row.get("councillor") or "").strip().lower(),
                    (row.get("register_url") or "").strip().lower(),
                )
                if all(key):
                    existing_pdfs.add(key)

    text_rows = []
    pdf_rows = []
    total = len(rows)
    for idx, (council, councillor, ward, register_url) in enumerate(rows, start=1):
        for url in _split_register_urls(register_url):
            key = (council.lower(), councillor.lower(), url.lower())
            if key in existing_texts or key in existing_pdfs:
                _log(f"[{idx}/{total}] Skipping {councillor} ({council})")
                continue
            if REQUEST_DELAY:
                time.sleep(REQUEST_DELAY)
            try:
                _log(f"[{idx}/{total}] Fetching register for {councillor} ({council})")
                resp = requests.get(url, headers=HEADERS, timeout=30)
                resp.raise_for_status()
            except Exception as exc:
                _log(f"[{idx}/{total}] Failed {councillor} ({council}): {exc}")
                continue

            content_type = (resp.headers.get("Content-Type") or "").lower()
            if _looks_like_pdf(url, content_type):
                pdf_rows.append((council, councillor, ward, url, content_type))
                continue

            text = _extract_text(resp.text)
            text_rows.append((council, councillor, ward, url, content_type, text))

    if text_rows:
        file_exists = os.path.exists(OUTPUT)
        with open(OUTPUT, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(
                    [
                        "council",
                        "councillor",
                        "ward",
                        "register_url",
                        "content_type",
                        "extracted_text",
                    ]
                )
            writer.writerows(text_rows)
        _log(f"Appended {len(text_rows)} rows to {OUTPUT}")

    if pdf_rows:
        file_exists = os.path.exists(PDF_OUTPUT)
        with open(PDF_OUTPUT, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(
                    ["council", "councillor", "ward", "register_url", "content_type"]
                )
            writer.writerows(pdf_rows)
        _log(f"Appended {len(pdf_rows)} rows to {PDF_OUTPUT}")


if __name__ == "__main__":
    main()
