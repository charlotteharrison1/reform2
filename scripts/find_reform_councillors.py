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
MISSING_COUNCILLORS = os.getenv("MISSING_COUNCILLORS_CSV", "missing_councillors.csv")
MISSING_COUNCILS = os.getenv("MISSING_COUNCILS_CSV", "missing-councils.csv")
USE_DEMOCRACY = os.getenv("USE_DEMOCRACY", "1") != "0"
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


def _slugify_council_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _build_index_url(council: str) -> str:
    slug = _slugify_council_name(council)
    if USE_DEMOCRACY:
        return f"https://democracy.{slug}.gov.uk/mgMemberIndex.aspx?bcr=1"
    return f"https://{slug}.moderngov.co.uk/mgMemberIndex.aspx?bcr=1"


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
            if council:
                index_url = _build_index_url(council)
                rows.append((council, council_url, index_url))

    existing_rows: list[tuple[str, str, str, str]] = []
    existing_set: set[tuple[str, str, str, str]] = set()
    existing_councils: set[str] = set()
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
                    existing_councils.add(council.lower())

    results: list[tuple[str, str, str, str]] = []
    failures: list[tuple[str, str, str]] = []
    successful_councils: set[str] = set()
    total = len(rows)
    for idx, (council, council_url, index_url) in enumerate(rows, start=1):
        if council.lower() in existing_councils:
            print(f"[{idx}/{total}] Skipping {council} (already logged)")
            continue
        try:
            print(f"[{idx}/{total}] Fetching {council}: {index_url}")
            matches = extract_reform_councillors(index_url)
        except Exception as exc:
            print(f"[{idx}/{total}] Failed {council}: {exc}")
            failures.append((council, index_url, str(exc)))
            continue
        successful_councils.add(council)
        print(f"[{idx}/{total}] Found {len(matches)} Reform UK councillor(s) for {council}")
        for name, ward, councillor_url in matches:
            key = (council.lower(), name.lower(), ward, councillor_url)
            if key in existing_set:
                continue
            existing_set.add(key)
            results.append((council, name, ward, councillor_url))

    combined = existing_rows + results
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["council", "councillor", "ward", "councillor_url"])
        writer.writerows(combined)

    with open(FAILURES, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["council", "councillor_index_url", "error"])
        writer.writerows(failures)

    print(f"Wrote {len(combined)} rows to {OUTPUT} (added {len(results)})")
    print(f"Wrote {len(failures)} rows to {FAILURES}")

    if successful_councils and os.path.exists(MISSING_COUNCILS):
        with open(MISSING_COUNCILS, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing = [
                (row.get("council") or "").strip()
                for row in reader
                if (row.get("council") or "").strip()
            ]
        kept = [c for c in existing if c not in successful_councils]
        with open(MISSING_COUNCILS, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["council"])
            for council in kept:
                writer.writerow([council])
        print(
            f"Updated {MISSING_COUNCILS} (removed {len(existing) - len(kept)} councils)"
        )

    if os.path.exists(MISSING_COUNCILLORS):
        def norm(text: str) -> str:
            return _normalize_whitespace(text).lower()

        found = set()
        for council, name, ward, url in combined:
            found.add((norm(council), norm(name), _normalize_whitespace(ward)))

        with open(MISSING_COUNCILLORS, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = [
                (
                    (row.get("council") or "").strip(),
                    (row.get("name") or "").strip(),
                    (row.get("ward") or "").strip(),
                )
                for row in reader
            ]
        kept_rows = []
        for council, name, ward in rows:
            key = (norm(council), norm(name), _normalize_whitespace(ward))
            if key in found:
                continue
            kept_rows.append((council, name, ward))
        with open(MISSING_COUNCILLORS, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["council", "name", "ward"])
            writer.writerows(kept_rows)
        print(
            f"Updated {MISSING_COUNCILLORS} (removed {len(rows) - len(kept_rows)} councillors)"
        )


if __name__ == "__main__":
    main()
