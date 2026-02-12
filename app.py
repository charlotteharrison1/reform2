"""Simple local search UI for councillor registers."""

from __future__ import annotations

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
    snippet = re.sub(r"([.!?])\\s+", r\"\\1<br>\", snippet)
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


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(debug=True, host="127.0.0.1", port=port)
