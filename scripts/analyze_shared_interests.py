"""Analyze shared interests across registers using fuzzy matching."""

from __future__ import annotations

import csv
import os
import re
from collections import defaultdict
from difflib import SequenceMatcher

INPUT = os.getenv("REGISTER_TEXTS_CSV", "reform_register_texts_clean.csv")
OUTPUT = os.getenv("SHARED_INTERESTS_CSV", "shared_interests.csv")
MIN_LEN = int(os.getenv("MIN_SENTENCE_LEN", "30"))
MAX_LEN = int(os.getenv("MAX_SENTENCE_LEN", "300"))
SIMILARITY = float(os.getenv("SIMILARITY_THRESHOLD", "0.88"))
MAX_EXAMPLES = int(os.getenv("MAX_EXAMPLES", "5"))


def _normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9\\s]", " ", text)
    return re.sub(r"\\s+", " ", text).strip()


def _split_sentences(text: str) -> list[str]:
    # Simple sentence splitting for register text.
    parts = re.split(r"[\\n\\r]+|[.!?]+", text)
    sentences = []
    for part in parts:
        cleaned = part.strip()
        if not cleaned:
            continue
        if len(cleaned) < MIN_LEN or len(cleaned) > MAX_LEN:
            continue
        sentences.append(cleaned)
    return sentences


def _similar(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def main() -> None:
    rows = []
    with open(INPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            council = (row.get("council") or "").strip()
            councillor = (row.get("councillor") or "").strip()
            ward = (row.get("ward") or "").strip()
            register_url = (row.get("register_url") or "").strip()
            text = (row.get("extracted_text") or "").strip()
            if not (council and councillor and register_url and text):
                continue
            rows.append((council, councillor, ward, register_url, text))

    # Build sentence candidates with blocking.
    blocks: dict[str, list[dict[str, str]]] = defaultdict(list)
    for council, councillor, ward, register_url, text in rows:
        for sentence in _split_sentences(text):
            normalized = _normalize(sentence)
            if not normalized:
                continue
            key = normalized[:24]
            blocks[key].append(
                {
                    "council": council,
                    "councillor": councillor,
                    "ward": ward,
                    "register_url": register_url,
                    "sentence": sentence,
                    "normalized": normalized,
                }
            )

    clusters: list[dict[str, object]] = []
    visited = set()

    for key, items in blocks.items():
        for i, item in enumerate(items):
            item_id = (key, i)
            if item_id in visited:
                continue
            visited.add(item_id)
            group = [item]
            for j in range(i + 1, len(items)):
                other = items[j]
                other_id = (key, j)
                if other_id in visited:
                    continue
                if _similar(item["normalized"], other["normalized"]) >= SIMILARITY:
                    visited.add(other_id)
                    group.append(other)

            # Keep only groups appearing in multiple registers
            register_ids = {(g["council"], g["councillor"], g["register_url"]) for g in group}
            if len(register_ids) < 2:
                continue

            clusters.append(
                {
                    "example": group[0]["sentence"],
                    "count": len(register_ids),
                    "examples": group[:MAX_EXAMPLES],
                }
            )

    # Sort by number of registers shared
    clusters.sort(key=lambda c: c["count"], reverse=True)

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "example_interest",
                "register_count",
                "example_councils",
                "example_councillors",
                "example_register_urls",
            ]
        )
        for cluster in clusters:
            examples = cluster["examples"]
            councils = "; ".join(e["council"] for e in examples)
            councillors = "; ".join(e["councillor"] for e in examples)
            urls = "; ".join(e["register_url"] for e in examples)
            writer.writerow(
                [
                    cluster["example"],
                    cluster["count"],
                    councils,
                    councillors,
                    urls,
                ]
            )

    print(f"Wrote {len(clusters)} shared-interest rows to {OUTPUT}")


if __name__ == "__main__":
    main()
