"""Find register-of-interests links from councillor profile pages."""

from __future__ import annotations

import csv
import os
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

INPUT = os.getenv("REFORM_COUNCILLORS_CSV", "reform_councillor_pages.csv")
OUTPUT = os.getenv("REGISTER_LINKS_CSV", "reform_register_links.csv")
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

PHRASES = (
    "register of interests",
    "register of interest",
    "register of member interests",
    "register of members interests",
    "members' interests",
    "member's interests",
    "declaration of interest",
    "declarations of interest",
    "pecuniary interests",
    "disclosable pecuniary interests",
)

URL_HINTS = (
    "mgdeclarationsubmission",
    "mgrofi",
    "registerofinterests",
    "register-of-interests",
    "register-of-members-interests",
)


def _log(msg: str) -> None:
    if LOG_LEVEL in {"INFO", "DEBUG"}:
        print(msg)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip().lower()


def _looks_like_register(text: str, href: str) -> bool:
    haystack = f"{text} {href}".lower()
    if any(hint in haystack for hint in URL_HINTS):
        return True
    return any(phrase in haystack for phrase in PHRASES)


def _extract_register_links(page_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    matches: list[str] = []
    for link in soup.find_all("a", href=True):
        text = _normalize(link.get_text(" ", strip=True))
        href = (link.get("href") or "").strip()
        if not href:
            continue
        if _looks_like_register(text, href):
            matches.append(urljoin(page_url, href))
            continue
        # Check nearby text (parent container) for register phrases.
        parent = link.find_parent()
        if parent is not None:
            parent_text = _normalize(parent.get_text(" ", strip=True))
            if _looks_like_register(parent_text, href):
                matches.append(urljoin(page_url, href))
    # Dedup while preserving order.
    seen = set()
    unique = []
    for url in matches:
        if url in seen:
            continue
        seen.add(url)
        unique.append(url)
    return unique


def main() -> None:
    rows = []
    with open(INPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            council = (row.get("council") or "").strip()
            name = (row.get("councillor") or "").strip()
            ward = (row.get("ward") or "").strip()
            url = (row.get("councillor_url") or "").strip()
            if council and name and url:
                rows.append((council, name, ward, url))

    existing = set()
    if os.path.exists(OUTPUT):
        with open(OUTPUT, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (
                    (row.get("council") or "").strip().lower(),
                    (row.get("councillor") or "").strip().lower(),
                    (row.get("councillor_url") or "").strip().lower(),
                )
                if all(key):
                    existing.add(key)

    out_rows = []
    total = len(rows)
    for idx, (council, name, ward, url) in enumerate(rows, start=1):
        key = (council.lower(), name.lower(), url.lower())
        if key in existing:
            _log(f"[{idx}/{total}] Skipping {name} ({council})")
            continue
        if REQUEST_DELAY:
            time.sleep(REQUEST_DELAY)
        try:
            _log(f"[{idx}/{total}] Fetching {name} ({council})")
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            _log(f"[{idx}/{total}] Failed {name} ({council}): {exc}")
            out_rows.append((council, name, ward, url, "", "fetch_error"))
            continue

        links = _extract_register_links(url, resp.text)
        if links:
            for link in links:
                out_rows.append((council, name, ward, url, link, ""))
            _log(f"[{idx}/{total}] Found {len(links)} register link(s)")
        else:
            out_rows.append((council, name, ward, url, "", "not_found"))
            _log(f"[{idx}/{total}] No register link found")

    # Append results
    file_exists = os.path.exists(OUTPUT)
    with open(OUTPUT, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(
                ["council", "councillor", "ward", "councillor_url", "register_url", "status"]
            )
        writer.writerows(out_rows)

    print(f"Wrote {len(out_rows)} rows to {OUTPUT}")


if __name__ == "__main__":
    main()
