"""Simple local search UI for councillor registers."""

from __future__ import annotations

import os
from typing import Any

from flask import Flask, render_template, request

from config import get_db_connection


app = Flask(__name__)


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
            LEFT(r.extracted_text, 800) AS snippet
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
    return render_template("index.html", query=query, results=results)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(debug=True, host="127.0.0.1", port=port)
