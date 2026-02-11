"""Discover 'your councillors' pages for each council and write a clean CSV.

Primary strategy: build the Moderngov member index URL from the council name
and check if it exists.
"""

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
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
USE_MODERNGOV = os.getenv("USE_MODERNGOV", "1") != "0"

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

def _debug(msg: str) -> None:
    if LOG_LEVEL == "DEBUG":
        print(msg)


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


def _text_has_keyword(text: str) -> bool:
    lowered = (text or "").lower()
    return any(k in lowered for k in KEYWORDS)


def _link_is_councillor_related(a) -> bool:
    try:
        href = (a.get("href") or "").strip()
        link_text = (a.get_text() or "").strip()
        if _text_has_keyword(link_text):
            return True
        if _text_has_keyword(href):
            return True
        parent_text = ""
        parent = a.find_parent()
        if parent is not None:
            parent_text = (parent.get_text(" ", strip=True) or "")[:200]
        if _text_has_keyword(parent_text):
            return True
    except Exception:
        return False
    return False


def _slugify_council_name(name: str) -> str:
    lowered = re.sub(r"[^\w\s-]", "", name.lower())
    lowered = re.sub(r"[\s_]+", "", lowered)
    return lowered


def _build_moderngov_index(name: str) -> str:
    slug = _slugify_council_name(name)
    return f"https://{slug}.moderngov.co.uk/mgMemberIndex.aspx?bcr=1"


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
            _debug(f"Crawling: {url}")
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
        except Exception:
            continue

        content_type = (resp.headers.get("Content-Type") or "").lower()
        if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
            # If content-type is missing or generic, do a light sniff before parsing
            if content_type and "text" not in content_type:
                continue
            sample = (resp.text or "")[:500].lower()
            if "<html" not in sample and "<!doctype html" not in sample:
                continue
        html = resp.text

        if looks_like_index_url(url) or page_matches_heading(html):
            found.append(url)
            # keep crawling to find other variants

        for link in extract_candidate_links(url, html):
            if is_internal(link, base_domain) and link not in seen:
                if depth + 1 <= MAX_DEPTH:
                    queue.append((link, depth + 1))

        if depth >= MAX_DEPTH:
            continue

        # Focused crawl: only follow links tied to "councillor" language
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            continue
        added = 0
        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if not _link_is_councillor_related(a):
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
        pages = []
        if USE_MODERNGOV:
            candidate = _build_moderngov_index(council)
            try:
                if REQUEST_DELAY:
                    time.sleep(REQUEST_DELAY)
                resp = requests.get(candidate, headers=HEADERS, timeout=20)
                if resp.ok:
                    results.append((council, url, candidate))
                    continue
            except Exception:
                pass
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
