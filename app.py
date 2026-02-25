"""Simple local search UI for councillor registers."""

from __future__ import annotations

import csv
import os
import re
from typing import Any

from flask import Flask, render_template, request
from markupsafe import Markup, escape

from config import get_db_connection


app = Flask(__name__)


def _highlight(text: str, term: str) -> Markup:
    if not text or not term:
        return Markup(escape(text or ""))
    pattern = re.compile(re.escape(term), re.IGNORECASE)
    safe_text = escape(text)
    highlighted = pattern.sub(
        lambda m: f"<mark class=\"hl\">{m.group(0)}</mark>", safe_text
    )
    return Markup(highlighted)


def _make_snippet(text: str, term: str, *, window_words: int = 24) -> Markup:
    if not text:
        return Markup("")
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not term:
        return _highlight(cleaned[:320], "")

    words = cleaned.split()
    if not words:
        return Markup("")

    lowered = [w.lower() for w in words]
    term_lower = term.lower()
    match_index = None
    for i, w in enumerate(lowered):
        if term_lower in w:
            match_index = i
            break

    if match_index is None:
        snippet = " ".join(words[:64])
        return _highlight(snippet, term)

    start = max(0, match_index - window_words)
    end = min(len(words), match_index + window_words + 1)
    snippet_words = words[start:end]
    snippet = " ".join(snippet_words)
    if start > 0:
        snippet = "… " + snippet
    if end < len(words):
        snippet = snippet + " …"

    # Add light formatting for readability: break after sentence endings.
    snippet = re.sub(r"([.!?])\\s+", r"\\1<br>", snippet)
    return _highlight(snippet, term)

def _query_registers(term: str) -> list[dict[str, Any]]:
    term = term.strip()
    if not term:
        return []

    like = f"%{term}%"
    sql = """
        SELECT
            c.id AS councillor_id,
            c.name,
            c.council,
            c.ward,
            r.register_url,
            r.fetched_at,
            r.content_type,
            r.extracted_text
        FROM councillor_registers r
        JOIN councillors c ON c.id = r.councillor_id
        WHERE
            c.name ILIKE %s
            OR c.council ILIKE %s
            OR c.ward ILIKE %s
            OR r.extracted_text ILIKE %s
        ORDER BY r.fetched_at DESC
        LIMIT 200
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (like, like, like, like))
            rows = cur.fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        results.append(
            {
                "councillor_id": row[0],
                "name": row[1],
                "council": row[2],
                "ward": row[3],
                "register_url": row[4],
                "fetched_at": row[5],
                "content_type": row[6],
                "snippet": row[7] or "",
            }
        )
    return results


def _load_shared_interests() -> list[dict[str, str]]:
    path = os.getenv("SHARED_INTERESTS_CSV", "shared_interests.csv")
    if not os.path.exists(path):
        return []
    rows: list[dict[str, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "example_interest": row.get("example_interest", "") or "",
                    "register_count": row.get("register_count", "") or "",
                    "example_councils": row.get("example_councils", "") or "",
                    "example_councillors": row.get("example_councillors", "") or "",
                    "example_register_urls": row.get("example_register_urls", "") or "",
                }
            )
    return rows


def _load_register_pdfs() -> list[dict[str, str]]:
    pdf_path = os.getenv("REGISTER_PDF_CSV", "reform_register_pdfs.csv")
    links_path = os.getenv("REGISTER_LINKS_CSV", "reform_register_links.csv")
    rows: list[dict[str, str]] = []

    def add_row(data: dict[str, str]) -> None:
        register_url = (data.get("register_url") or "").strip()
        if not register_url:
            return
        rows.append(
            {
                "council": (data.get("council") or "").strip(),
                "councillor": (data.get("councillor") or "").strip(),
                "ward": (data.get("ward") or "").strip(),
                "register_url": register_url,
                "content_type": (data.get("content_type") or "").strip(),
                "source": (data.get("source") or "").strip(),
            }
        )

    if os.path.exists(pdf_path):
        with open(pdf_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row = dict(row)
                row["source"] = "register_pdfs"
                add_row(row)

    if os.path.exists(links_path):
        with open(links_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                register_url = (row.get("register_url") or "").strip()
                if not register_url or ".pdf" not in register_url.lower():
                    continue
                row = dict(row)
                row["source"] = "register_links"
                add_row(row)

    # Deduplicate by (council, councillor, register_url)
    seen: set[tuple[str, str, str]] = set()
    unique_rows: list[dict[str, str]] = []
    for row in rows:
        key = (
            row["council"].lower(),
            row["councillor"].lower(),
            row["register_url"].lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)

    return unique_rows


@app.route("/", methods=["GET"])
def index() -> str:
    query = request.args.get("q", "").strip()
    results = _query_registers(query) if query else []
    return render_template(
        "index.html",
        query=query,
        results=results,
        highlight=_highlight,
        make_snippet=_make_snippet,
    )


@app.route("/shared-interests", methods=["GET"])
def shared_interests() -> str:
    rows = _load_shared_interests()
    return render_template("shared_interests.html", rows=rows)


@app.route("/pdfs", methods=["GET"])
def pdfs() -> str:
    query = request.args.get("q", "").strip()
    rows = _load_register_pdfs()
    if query:
        q = query.lower()
        rows = [
            row
            for row in rows
            if q in row["council"].lower()
            or q in row["councillor"].lower()
            or q in row["ward"].lower()
            or q in row["register_url"].lower()
        ]
    return render_template("pdfs.html", rows=rows, total=len(rows), query=query)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(debug=True, host="127.0.0.1", port=port)
