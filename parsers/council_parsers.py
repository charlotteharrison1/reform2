"""Generic council page parsers."""

import re
from typing import Iterable, Optional
from urllib.parse import parse_qs, unquote, urlparse
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


_REGISTER_PATTERNS = [
    r"register of interests",
    r"register of member(?:'|’)s? interests",
    r"members?['’] interests",
    r"declarations? of interest",
    r"pecuniary interests",
    r"disclosable pecuniary interests",
]

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}


def _looks_like_register_link(text: str, href: str) -> bool:
    """Return True when link text or URL suggests a register page."""

    haystack = f"{text} {href}".strip().lower()
    for pattern in _REGISTER_PATTERNS:
        if re.search(pattern, haystack, flags=re.IGNORECASE):
            return True
    return False


def _collect_register_links(base_url: str, html: str) -> list[str]:
    """Collect candidate register links from a page."""

    soup = BeautifulSoup(html, "html.parser")
    matches: list[str] = []
    for link in soup.find_all("a", href=True):
        text = (link.get_text() or "").strip()
        href = (link.get("href") or "").strip()
        if not href:
            continue
        if _looks_like_register_link(text, href):
            matches.append(urljoin(base_url, href))
    return matches


def _normalize_name(value: str) -> str:
    """Normalize a name for fuzzy matching."""

    value = re.sub(r"[^a-z0-9\\s]", " ", value.lower())
    return re.sub(r"\\s+", " ", value).strip()


def find_councillor_links(base_url: str, html: str, name: str) -> list[str]:
    """Find links on a page that likely belong to a councillor."""

    soup = BeautifulSoup(html, "html.parser")
    target = _normalize_name(name)
    matches: list[str] = []

    for link in soup.find_all("a", href=True):
        text = (link.get_text() or "").strip()
        href = (link.get("href") or "").strip()
        if not href:
            continue
        if target and target in _normalize_name(text):
            matches.append(urljoin(base_url, href))

    return matches


def _extract_duckduckgo_url(href: str) -> Optional[str]:
    """Extract a target URL from DuckDuckGo redirect links."""

    if "duckduckgo.com/l/?" not in href:
        return None

    parsed = urlparse(href)
    query = parse_qs(parsed.query)
    target = query.get("uddg", [None])[0]
    if not target:
        return None
    return unquote(target)


def search_web(query: str, *, max_results: int = 5) -> Iterable[str]:
    """Return a short list of result URLs from DuckDuckGo HTML search."""

    search_url = "https://duckduckgo.com/html/"
    response = requests.get(
        search_url,
        params={"q": query},
        headers=_DEFAULT_HEADERS,
        timeout=20,
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    results: list[str] = []

    for link in soup.select("a.result__a[href]"):
        href = link.get("href") or ""
        target = _extract_duckduckgo_url(href) or href
        if not target:
            continue
        results.append(target)
        if len(results) >= max_results:
            break

    return results


def find_council_register_pages(council: str) -> list[str]:
    """Find candidate register pages for a council via web search."""

    queries = [
        f'{council} council "register of interests"',
        f'{council} council "members interests"',
        f'{council} council "declarations of interest"',
    ]

    found: list[str] = []
    seen: set[str] = set()

    for query in queries:
        for result_url in search_web(query, max_results=8):
            if not result_url or result_url in seen:
                continue
            seen.add(result_url)

            if _looks_like_register_link(result_url, result_url):
                found.append(result_url)
                continue

            try:
                response = requests.get(
                    result_url, headers=_DEFAULT_HEADERS, timeout=20
                )
                response.raise_for_status()
            except Exception:
                continue

            for link in _collect_register_links(result_url, response.text):
                if link not in seen:
                    seen.add(link)
                    found.append(link)

    return found
