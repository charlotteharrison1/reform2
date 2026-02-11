"""Extract Reform UK councillor pages from Moderngov member index pages."""

from __future__ import annotations

import csv
import os
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

INPUT = os.getenv("COUNCILLOR_INDEX_CSV", "council_councillor_pages.csv")
OUTPUT = os.getenv("REFORM_COUNCILLORS_CSV", "reform_councillor_pages.csv")
FAILURES = os.getenv("REFORM_COUNCILLOR_FAILURES_CSV", "reform_councillor_failures.csv")
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}


def _debug(msg: str) -> None:
    if LOG_LEVEL == "DEBUG":
        print(msg)


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def extract_reform_councillors(index_url: str) -> list[tuple[str, str, str]]:
    """Return list of (name, ward, councillor_url) for Reform UK entries."""
    if REQUEST_DELAY:
        time.sleep(REQUEST_DELAY)
    try:
        resp = requests.get(index_url, headers=HEADERS, timeout=30)
    except requests.RequestException as exc:
        raise RuntimeError(f"Request failed: {exc}") from exc
    if not resp.ok:
        raise RuntimeError(f"Non-200 response: {resp.status_code}")
    html = resp.text

    soup = BeautifulSoup(html, "html.parser")
    results: list[tuple[str, str, str]] = []

    for li in soup.find_all("li"):
        text = _normalize_whitespace(li.get_text(" ", strip=True))
        if "reform uk" not in text.lower():
            continue
        a = li.find("a", href=True)
        if not a:
            continue
        href = (a.get("href") or "").strip()
        if not href:
            continue
        name = _normalize_whitespace(a.get_text(" ", strip=True))
        # Some entries include "Councillor X" in the anchor text.
        name = re.sub(r"^Councillor\\s+", "", name, flags=re.IGNORECASE)
        ward = ""
        for p in li.find_all("p"):
            p_text = _normalize_whitespace(p.get_text(" ", strip=True))
            if not p_text:
                continue
            # Skip the party line, keep the ward line.
            if "reform uk" in p_text.lower():
                continue
            ward = p_text
            break
        councillor_url = urljoin(index_url, href)
        results.append((name, ward, councillor_url))

    return results


def main() -> None:
    rows = []
    with open(INPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            council = (row.get("council") or "").strip()
            council_url = (row.get("council_url") or "").strip()
            index_url = (row.get("councillor_index_url") or "").strip()
            if council and index_url:
                rows.append((council, council_url, index_url))

    results: list[tuple[str, str, str, str]] = []
    failures: list[tuple[str, str, str]] = []
    total = len(rows)
    for idx, (council, council_url, index_url) in enumerate(rows, start=1):
        try:
            print(f"[{idx}/{total}] Fetching {council}: {index_url}")
            matches = extract_reform_councillors(index_url)
        except Exception as exc:
            print(f"[{idx}/{total}] Failed {council}: {exc}")
            failures.append((council, index_url, str(exc)))
            continue
        print(f"[{idx}/{total}] Found {len(matches)} Reform UK councillor(s) for {council}")
        for name, ward, councillor_url in matches:
            results.append((council, name, ward, councillor_url))

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["council", "councillor", "ward", "councillor_url"])
        writer.writerows(results)

    with open(FAILURES, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["council", "councillor_index_url", "error"])
        writer.writerows(failures)

    print(f"Wrote {len(results)} rows to {OUTPUT}")
    print(f"Wrote {len(failures)} rows to {FAILURES}")


if __name__ == "__main__":
    main()
