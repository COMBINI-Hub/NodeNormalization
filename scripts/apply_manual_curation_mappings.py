#!/usr/bin/env python3
"""
Apply manually curated COMBO mappings from combo_manual_curation.tsv.

Rows with selected_identifier are converted to pipeline-compatible mappings.
Remaining unresolved rows are carried forward.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, List, Set


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply manual curation mappings.")
    parser.add_argument(
        "--manual-input",
        default="combo_manual_curation.tsv",
        help="Manual curation TSV with selected_identifier columns.",
    )
    parser.add_argument(
        "--unresolved-input",
        default="combo_unresolved_after_cam_lexicon.tsv",
        help="Unresolved TSV before manual curation.",
    )
    parser.add_argument(
        "--promoted-output",
        default="combo_manual_curated_mappings.tsv",
        help="Output promoted mappings from manual curation.",
    )
    parser.add_argument(
        "--remaining-output",
        default="combo_unresolved_after_manual_curation.tsv",
        help="Remaining unresolved after applying manual curation.",
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
    manual_path = Path(args.manual_input)
    unresolved_rows = read_tsv(Path(args.unresolved_input))

    if not manual_path.exists():
        print(f"[warn] Manual curation file not found, skipping: {manual_path}")
        write_tsv(
            Path(args.promoted_output),
            [],
            [
                "input_curie",
                "preferred_identifier",
                "preferred_label",
                "confidence",
                "method",
                "mapping_relation",
                "combo_concept_id",
                "combo_label",
                "source_identifier",
                "evidence",
            ],
        )
        write_tsv(
            Path(args.remaining_output),
            unresolved_rows,
            ["combo_concept_id", "combo_label", "synonyms", "reason"],
        )
        return

    manual_rows = read_tsv(manual_path)
    promoted_rows: List[Dict[str, object]] = []
    promoted_ids: Set[str] = set()
    for row in manual_rows:
        cid = (row.get("combo_concept_id") or "").strip()
        selected_identifier = (row.get("selected_identifier") or "").strip()
        if not cid or not selected_identifier:
            continue
        selected_label = (row.get("selected_label") or "").strip()
        mapping_relation = (row.get("mapping_relation") or "").strip() or "manual_curated"
        notes = (row.get("curator_notes") or "").strip()
        promoted_ids.add(cid)
        promoted_rows.append(
            {
                "input_curie": f"COMBO:{cid}",
                "preferred_identifier": selected_identifier,
                "preferred_label": selected_label,
                "confidence": 0.99,
                "method": "manual_curation",
                "mapping_relation": mapping_relation,
                "combo_concept_id": cid,
                "combo_label": (row.get("combo_label") or "").strip(),
                "source_identifier": f"COMBO:{cid}",
                "evidence": f"manual_curation:{notes}" if notes else "manual_curation",
            }
        )

    remaining_rows = [
        row for row in unresolved_rows if (row.get("combo_concept_id") or "").strip() not in promoted_ids
    ]

    write_tsv(
        Path(args.promoted_output),
        promoted_rows,
        [
            "input_curie",
            "preferred_identifier",
            "preferred_label",
            "confidence",
            "method",
            "mapping_relation",
            "combo_concept_id",
            "combo_label",
            "source_identifier",
            "evidence",
        ],
    )
    write_tsv(
        Path(args.remaining_output),
        remaining_rows,
        ["combo_concept_id", "combo_label", "synonyms", "reason"],
    )

    print(f"[ok] Manual rows read: {len(manual_rows)}")
    print(f"[ok] Manual mappings applied: {len(promoted_rows)}")
    print(f"[ok] Unresolved before manual: {len(unresolved_rows)}")
    print(f"[ok] Unresolved after manual: {len(remaining_rows)}")
    print(f"[ok] Wrote: {args.promoted_output}")
    print(f"[ok] Wrote: {args.remaining_output}")


if __name__ == "__main__":
    main()
