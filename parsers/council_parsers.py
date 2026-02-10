"""Generic council page parsers."""

import logging
import re
import xml.etree.ElementTree as ET
from collections import deque
from typing import Iterable, Optional
from urllib.parse import parse_qs, unquote, urlparse, urljoin

import requests
from bs4 import BeautifulSoup


_REGISTER_PATTERNS = [
    r"register of interest",
    r"register of interests",
    r"register of member(?:'|’)s? interests",
    r"members?['’] interests",
    r"declarations? of interest",
    r"pecuniary interests",
    r"disclosable pecuniary interests",
]

logger = logging.getLogger(__name__)

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


def _collect_pdf_links(base_url: str, html: str) -> list[str]:
    """Collect PDF links from a page."""

    soup = BeautifulSoup(html, "html.parser")
    matches: list[str] = []
    for link in soup.find_all("a", href=True):
        href = (link.get("href") or "").strip()
        if not href:
            continue
        if ".pdf" in href.lower():
            matches.append(urljoin(base_url, href))
    return matches


def find_pdf_links(base_url: str, html: str) -> list[str]:
    """Public wrapper to collect PDF links from a page."""

    return _collect_pdf_links(base_url, html)


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


def _council_tokens(council: str) -> list[str]:
    tokens = re.split(r"[^a-z0-9]+", council.lower())
    return [t for t in tokens if len(t) >= 4]


def _url_matches_council(url: str, council: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower()
    tokens = _council_tokens(council)
    if not tokens:
        return False

    if host.endswith(".gov.uk"):
        return any(token in host for token in tokens)

    if "moderngov" in host or "democracy" in host:
        return any(token in host or token in path for token in tokens)

    return any(token in host or token in path for token in tokens)


def find_council_homepage(council: str) -> Optional[str]:
    """Find a likely council homepage using web search."""

    queries = [
        f"{council} council",
        f"{council} council website",
        f"{council} local authority",
    ]
    for query in queries:
        for result_url in search_web(query, max_results=8):
            if not result_url:
                continue
            if _url_matches_council(result_url, council):
                return result_url
    return None


_CRAWL_KEYWORDS = (
    "register",
    "interest",
    "member",
    "councillor",
    "democracy",
    "modern",
    "moderngov",
    "committee",
    "governance",
    "declaration",
)


def crawl_council_register_pages(
    council: str,
    *,
    homepage: Optional[str] = None,
    max_pages: int = 40,
    max_depth: int = 2,
) -> list[str]:
    """Crawl a council website for register pages and PDFs."""

    homepage = homepage or find_council_homepage(council)
    if not homepage:
        return []

    parsed_home = urlparse(homepage)
    base_host = parsed_home.hostname or ""
    base_domain = ".".join(base_host.split(".")[-3:]) if base_host.count(".") >= 2 else base_host
    logger.info("Council homepage for %s: %s", council, homepage)

    def is_internal(url: str) -> bool:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        return host.endswith(base_domain)

    seed_paths = [
        "/a-z",
        "/az",
        "/documents",
        "/document-library",
        "/democracy",
        "/moderngov",
        "/committee",
        "/committees",
        "/your-council",
        "/council",
    ]
    seeds = [homepage]
    for path in seed_paths:
        seeds.append(urljoin(homepage, path))

    def crawl(max_pages_limit: int, max_depth_limit: int) -> list[str]:
        queue = deque([(seed, 0) for seed in seeds])
        seen: set[str] = set()
        found: list[str] = []

        while queue and len(seen) < max_pages_limit:
            url, depth = queue.popleft()
            if url in seen:
                continue
            seen.add(url)

            try:
                response = requests.get(url, headers=_DEFAULT_HEADERS, timeout=20)
                response.raise_for_status()
            except Exception:
                continue

            html = response.text
            for link in _collect_register_links(url, html):
                if link not in found:
                    found.append(link)

            for link in _collect_pdf_links(url, html):
                if link not in found:
                    found.append(link)

            if depth >= max_depth_limit:
                continue

            soup = BeautifulSoup(html, "html.parser")
            links = list(soup.find_all("a", href=True))

            # From the homepage, allow a broader set of internal links.
            if depth == 0:
                extra_added = 0
                for link in links:
                    href = (link.get("href") or "").strip()
                    if not href:
                        continue
                    next_url = urljoin(url, href)
                    if not is_internal(next_url):
                        continue
                    if next_url in seen:
                        continue
                    queue.append((next_url, depth + 1))
                    extra_added += 1
                    if extra_added >= 20:
                        break

            for link in links:
                href = (link.get("href") or "").strip()
                if not href:
                    continue
                text = (link.get_text() or "").strip().lower()
                href_lower = href.lower()
                if not any(keyword in text or keyword in href_lower for keyword in _CRAWL_KEYWORDS):
                    continue

                next_url = urljoin(url, href)
                if not is_internal(next_url):
                    continue
                if next_url in seen:
                    continue
                queue.append((next_url, depth + 1))

        return found

    found = crawl(max_pages, max_depth)
    if not found and max_depth < 3:
        logger.info("Expanding crawl for %s (depth=%s, pages=%s)", council, max_depth + 1, max_pages * 2)
        found = crawl(max_pages * 2, max_depth + 1)

    return found


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


def _normalize_bing_href(href: str) -> Optional[str]:
    href = (href or "").strip()
    if not href:
        return None
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return urljoin("https://www.bing.com", href)
    return None


def _search_bing(query: str, *, max_results: int) -> list[str]:
    # Prefer Bing's RSS output because standard SERP HTML often hides result URLs.
    rss_url = "https://www.bing.com/search"
    response = requests.get(
        rss_url,
        params={"q": query, "format": "rss"},
        headers=_DEFAULT_HEADERS,
        timeout=20,
    )
    response.raise_for_status()

    results: list[str] = []
    try:
        root = ET.fromstring(response.text)
        for item in root.findall(".//item"):
            link = item.findtext("link")
            if link:
                results.append(link.strip())
            if len(results) >= max_results:
                break
    except ET.ParseError:
        logger.debug("Bing RSS parse failed for %s", query)

    if results:
        return results

    # Fallback: attempt to parse the HTML if RSS fails.
    html_response = requests.get(
        rss_url,
        params={"q": query},
        headers=_DEFAULT_HEADERS,
        timeout=20,
    )
    html_response.raise_for_status()

    soup = BeautifulSoup(html_response.text, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else ""

    for link in soup.select("li.b_algo h2 a[href]"):
        href = _normalize_bing_href(link.get("href") or "")
        if not href:
            continue
        results.append(href)
        if len(results) >= max_results:
            break

    if not results:
        logger.debug(
            "Bing returned no results for %s (title=%s, url=%s)",
            query,
            title,
            html_response.url,
        )

    return results


def _search_brave(query: str, *, max_results: int) -> list[str]:
    search_url = "https://search.brave.com/search"
    response = requests.get(
        search_url,
        params={"q": query},
        headers=_DEFAULT_HEADERS,
        timeout=20,
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else ""
    results: list[str] = []

    for link in soup.select("a.result-title[href]"):
        href = link.get("href") or ""
        if not href:
            continue
        results.append(href)
        if len(results) >= max_results:
            break

    if not results:
        logger.debug(
            "Brave returned no results for %s (title=%s, url=%s)",
            query,
            title,
            response.url,
        )

    return results


def search_web(query: str, *, max_results: int = 5) -> Iterable[str]:
    """Return a short list of result URLs from a web search provider."""

    try:
        results = _search_bing(query, max_results=max_results)
        logger.debug("Bing results for %s: %s", query, results)
        if results:
            return results
    except Exception as exc:
        logger.debug("Bing search failed for %s: %s", query, exc)

    try:
        results = _search_brave(query, max_results=max_results)
        logger.debug("Brave results for %s: %s", query, results)
        return results
    except Exception as exc:
        logger.debug("Brave search failed for %s: %s", query, exc)
        return []


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
                if link in seen:
                    continue
                if not _url_matches_council(link, council):
                    continue
                seen.add(link)
                found.append(link)

    return found


def find_register_pages_for_councillor(
    name: str, council: str, ward: Optional[str] = None
) -> list[str]:
    """Search the web for register pages related to a councillor."""

    ward_clause = f' "{ward}"' if ward else ""
    base_name = f'"{name}"'
    base_council = f'"{council}"'
    council_slug = council.lower().replace(" ", "")
    site_hint = f"site:{council_slug}.gov.uk"

    # Progressive fallback search: start specific, then broaden.
    queries = [
        f"{base_name}{ward_clause} {base_council} \"register of interests\"",
        f"{base_name} {council} register of interests",
        f"{base_name} {council} councillor",
        f"{base_name} register of interests",
        f"{council} council register of interests",
        f"{council} council members interests",
        f"{council} council declarations of interest",
        f"{site_hint} register of interests",
        f"site:moderngov.co.uk {council} register of interests",
    ]

    found: list[str] = []
    seen: set[str] = set()

    for query in queries:
        for result_url in search_web(query, max_results=8):
            if not result_url or result_url in seen:
                continue
            seen.add(result_url)

            if not _url_matches_council(result_url, council):
                continue

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
