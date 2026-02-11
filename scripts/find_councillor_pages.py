"""Discover 'your councillors' pages for each council and write a clean CSV."""

from __future__ import annotations

import csv
import os
import re
import time
import warnings
from collections import deque
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

INPUT = os.getenv("COUNCIL_URLS_CSV", "council_verified_urls.csv")
OUTPUT = os.getenv("COUNCILLOR_PAGES_CSV", "council_councillor_pages.csv")
MAX_PAGES = int(os.getenv("COUNCILLOR_CRAWL_PAGES", "120"))
MAX_DEPTH = int(os.getenv("COUNCILLOR_CRAWL_DEPTH", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

KEYWORDS = (
    "your councillors",
    "your-councillors",
    "councillors",
    "council members",
    "members",
    "mgmemberindex",
    "mguserinfo",
)


def is_internal(url: str, base_domain: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith(base_domain)


def looks_like_index_url(url: str) -> bool:
    lowered = url.lower()
    return any(k in lowered for k in KEYWORDS)


def page_matches_heading(html: str) -> bool:
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return False
    headings = []
    for tag in ("h1", "h2", "title"):
        for node in soup.find_all(tag):
            text = (node.get_text() or "").strip().lower()
            if text:
                headings.append(text)
    return any(
        "your councillors" in h
        or "your councilors" in h
        or "councillors" in h
        or "council members" in h
        for h in headings
    )


def extract_candidate_links(base_url: str, html: str) -> list[str]:
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return []
    links = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        text = (a.get_text() or "").strip().lower()
        if any(k in text for k in KEYWORDS) or any(k in href.lower() for k in KEYWORDS):
            links.append(urljoin(base_url, href))
    return links


def crawl_for_councillor_page(start_url: str) -> list[str]:
    parsed = urlparse(start_url)
    base_host = parsed.hostname or ""
    base_domain = (
        ".".join(base_host.split(".")[-3:]) if base_host.count(".") >= 2 else base_host
    )

    queue = deque([(start_url, 0)])
    seen = set()
    found = []

    while queue and len(seen) < MAX_PAGES:
        url, depth = queue.popleft()
        if url in seen:
            continue
        seen.add(url)

        if REQUEST_DELAY:
            time.sleep(REQUEST_DELAY)
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
        except Exception:
            continue

        content_type = (resp.headers.get("Content-Type") or "").lower()
        if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
            continue
        html = resp.text

        if looks_like_index_url(url) or page_matches_heading(html):
            found.append(url)
            # keep crawling to find other variants

        for link in extract_candidate_links(url, html):
            if is_internal(link, base_domain) and link not in seen:
                queue.append((link, depth + 1))

        if depth >= MAX_DEPTH:
            continue

        # General crawl: follow a limited number of internal links per page
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            continue
        added = 0
        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            next_url = urljoin(url, href)
            if not is_internal(next_url, base_domain):
                continue
            if next_url in seen:
                continue
            queue.append((next_url, depth + 1))
            added += 1
            if added >= 25:
                break

    # Dedup while preserving order
    seen_urls = set()
    unique = []
    for u in found:
        if u in seen_urls:
            continue
        seen_urls.add(u)
        unique.append(u)
    return unique


def main() -> None:
    rows = []
    with open(INPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            council = (row.get("council") or "").strip()
            url = (row.get("council_url") or "").strip()
            if council and url:
                rows.append((council, url))

    results = []
    for council, url in rows:
        pages = crawl_for_councillor_page(url)
        if pages:
            for p in pages:
                results.append((council, url, p))
        else:
            results.append((council, url, ""))

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["council", "council_url", "councillor_index_url"])
        writer.writerows(results)

    print(f"Wrote {len(results)} rows to {OUTPUT}")


if __name__ == "__main__":
    main()
