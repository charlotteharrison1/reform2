"""Generic council page parsers."""

from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


def find_register_url_generic(profile_url: str) -> Optional[str]:
    """Find a "Register of Interests" link on a councillor profile page.

    Returns the absolute URL if found, otherwise None.
    """

    if not profile_url:
        return None

    response = requests.get(profile_url, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Search for anchor tags whose text or href indicates a register of interests.
    for link in soup.find_all("a", href=True):
        text = (link.get_text() or "").strip().lower()
        href = (link.get("href") or "").strip()
        href_lower = href.lower()

        if "register of interests" in text or "register of interests" in href_lower:
            return urljoin(profile_url, href)

    return None
