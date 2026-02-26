#!/usr/bin/env python3
"""
Prepare manual curation template from MRCONSO review rows.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare manual curation TSV from review rows.")
    parser.add_argument(
        "--review-input",
        default="combo_mrconso_exact_review.remaining.tsv",
        help="Review TSV to convert into manual curation template.",
    )
    parser.add_argument(
        "--output",
        default="combo_manual_curation.tsv",
        help="Output manual curation TSV.",
    )
    return parser.parse_args()


def read_tsv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle, delimiter="\t")]


def write_tsv(path: Path, rows: Iterable[Dict[str, object]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    review_rows = read_tsv(Path(args.review_input))
    out_rows = []
    for row in review_rows:
        out_rows.append(
            {
                "combo_concept_id": (row.get("combo_concept_id") or "").strip(),
                "combo_label": (row.get("combo_label") or "").strip(),
                "synonyms": (row.get("synonyms") or "").strip(),
                "candidate_count": (row.get("candidate_count") or "").strip(),
                "candidates": (row.get("candidates") or "").strip(),
                "selected_identifier": "",
                "selected_label": "",
                "mapping_relation": "",
                "curator_notes": "",
            }
        )
    write_tsv(
        Path(args.output),
        out_rows,
        [
            "combo_concept_id",
            "combo_label",
            "synonyms",
            "candidate_count",
            "candidates",
            "selected_identifier",
            "selected_label",
            "mapping_relation",
            "curator_notes",
        ],
    )
    print(f"[ok] Review rows read: {len(review_rows)}")
    print(f"[ok] Wrote manual curation template: {args.output}")


if __name__ == "__main__":
    main()
