"""Scrape councillor index pages from a CSV and log new failures."""

from __future__ import annotations

import csv
import os
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

INPUT = os.getenv("INDEX_PAGES_CSV", "final_councillors_index_pages.csv")
OUTPUT = os.getenv("REFORM_COUNCILLORS_CSV", "reform_councillor_pages.csv")
FAILURES = os.getenv("COUNCILLOR_FAILURES_CSV", "councillor_failures.csv")
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


def _normalize_whitespace(text: str) -> str:
    return " ".join((text or "").split())


def _extract_reform(html: str, base_url: str) -> list[tuple[str, str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    results: list[tuple[str, str, str]] = []
    for li in soup.find_all("li"):
        text = _normalize_whitespace(li.get_text(" ", strip=True))
        if "reform" not in text.lower():
            continue
        a = li.find("a", href=True)
        if not a:
            continue
        href = (a.get("href") or "").strip()
        if not href:
            continue
        name = _normalize_whitespace(a.get_text(" ", strip=True))
        if name.lower().startswith("councillor "):
            name = name[len("councillor ") :]
        ward = ""
        for p in li.find_all("p"):
            p_text = _normalize_whitespace(p.get_text(" ", strip=True))
            if not p_text:
                continue
            if "reform" in p_text.lower():
                continue
            ward = p_text
            break
        councillor_url = urljoin(base_url, href)
        results.append((name, ward, councillor_url))
    return results


def main() -> None:
    rows = []
    with open(INPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        url_key = ""
        for header in headers:
            if header and header.lower() != "council":
                url_key = header
                break
        for row in reader:
            council = (row.get("council") or "").strip()
            index_url = (row.get(url_key) or "").strip()
            if council and index_url:
                rows.append((council, index_url))

    existing_set = set()
    existing_rows = []
    if os.path.exists(OUTPUT):
        with open(OUTPUT, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                council = (row.get("council") or "").strip()
                name = (row.get("councillor") or "").strip()
                ward = (row.get("ward") or "").strip()
                url = (row.get("councillor_url") or "").strip()
                if council and name and url:
                    existing_rows.append((council, name, ward, url))
                    existing_set.add((council.lower(), name.lower(), ward, url))

    failures = []
    new_rows = []
    total = len(rows)
    for idx, (council, index_url) in enumerate(rows, start=1):
        if REQUEST_DELAY:
            time.sleep(REQUEST_DELAY)
        try:
            _log(f"[{idx}/{total}] Fetching {council}: {index_url}")
            resp = requests.get(index_url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            _log(f"[{idx}/{total}] Failed {council}: {exc}")
            failures.append((council, index_url, str(exc)))
            continue

        matches = _extract_reform(resp.text, index_url)
        _log(f"[{idx}/{total}] Found {len(matches)} Reform councillor(s)")
        for name, ward, url in matches:
            key = (council.lower(), name.lower(), ward, url)
            if key in existing_set:
                continue
            existing_set.add(key)
            new_rows.append((council, name, ward, url))

    combined = existing_rows + new_rows
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["council", "councillor", "ward", "councillor_url", "register_url"])
        writer.writerows(combined)

    with open(FAILURES, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["council", "index_url", "error"])
        writer.writerows(failures)

    print(f"Wrote {len(new_rows)} new rows to {OUTPUT}")
    print(f"Wrote {len(failures)} rows to {FAILURES}")


if __name__ == "__main__":
    main()
