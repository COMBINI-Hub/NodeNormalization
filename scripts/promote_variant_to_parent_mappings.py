#!/usr/bin/env python3
"""
Promote unresolved variant labels to mapped parent concepts.

This script identifies labels such as "X Music Therapy", "Y Acupuncture",
"Z Moxibustion", etc., and maps them to already-mapped parent concepts.
Mappings emitted by this script are explicitly non-exact.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote unresolved variants to mapped parent concepts.")
    parser.add_argument(
        "--unresolved-input",
        default="combo_unresolved_after_mrconso_review.tsv",
        help="Unresolved concepts TSV to process.",
    )
    parser.add_argument(
        "--mapped-input",
        default="combo_seed_mappings.with_mrconso_exact_and_review.tsv",
        help="Mapped concepts TSV used to resolve parent labels to identifiers.",
    )
    parser.add_argument(
        "--promoted-output",
        default="combo_variant_parent_mappings.tsv",
        help="Output promoted non-exact mappings TSV.",
    )
    parser.add_argument(
        "--remaining-output",
        default="combo_unresolved_after_variant_parent.tsv",
        help="Unresolved TSV after variant-parent promotion.",
    )
    return parser.parse_args()


def normalize_text(text: str) -> str:
    value = (text or "").lower().replace("_", " ").strip()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


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


def build_parent_index(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """
    Build normalized label -> preferred mapping record index.
    Prefer higher confidence rows when duplicates exist.
    """
    index: Dict[str, Dict[str, str]] = {}
    for row in rows:
        label = normalize_text(row.get("combo_label") or row.get("preferred_label") or "")
        if not label:
            continue
        preferred_identifier = (row.get("preferred_identifier") or "").strip()
        preferred_label = (row.get("preferred_label") or row.get("combo_label") or "").strip()
        if not preferred_identifier:
            continue
        rec = {
            "preferred_identifier": preferred_identifier,
            "preferred_label": preferred_label,
            "confidence": str(row.get("confidence") or ""),
        }
        cur = index.get(label)
        if cur is None:
            index[label] = rec
        else:
            try:
                cur_conf = float(cur.get("confidence") or 0.0)
            except ValueError:
                cur_conf = 0.0
            try:
                new_conf = float(rec.get("confidence") or 0.0)
            except ValueError:
                new_conf = 0.0
            if new_conf > cur_conf:
                index[label] = rec
    return index


def derive_parent_label(label: str) -> Optional[Tuple[str, str]]:
    """
    Return (parent_label, relation) for known variant patterns.
    relation is used as non-exact match annotation.
    """
    n = normalize_text(label)
    if not n:
        return None

    # Ordered from specific to general.
    patterns = [
        (r"^.+ music therapy$", "music therapy", "narrow_to_parent"),
        (r"^.+ art therapy$", "art therapy", "narrow_to_parent"),
        (r"^.+ acupuncture$", "acupuncture", "narrow_to_parent"),
        (r"^.+ moxibustion$", "moxibustion", "narrow_to_parent"),
        (r"^.+ reflexology$", "reflexology", "narrow_to_parent"),
        (r"^.+ biofeedback$", "biofeedback", "narrow_to_parent"),
        (r"^.+ yoga$", "yoga", "narrow_to_parent"),
        (r"^.+ meditation$", "meditation", "narrow_to_parent"),
        (r"^.+ qigong$", "qigong", "narrow_to_parent"),
        (r"^.+ massage$", "massage", "narrow_to_parent"),
        (r"^.+ breathing technique$", "breathing exercise", "close_match"),
        (r"^.+ breathing$", "breathing exercise", "close_match"),
        (r"^.+ intervention$", "intervention", "close_match"),
        (r"^.+ therapy$", "therapy", "close_match"),
    ]
    for pat, parent, relation in patterns:
        if re.match(pat, n):
            return parent, relation
    return None


def main() -> None:
    args = parse_args()
    unresolved_rows = read_tsv(Path(args.unresolved_input))
    mapped_rows = read_tsv(Path(args.mapped_input))
    parent_index = build_parent_index(mapped_rows)

    promoted_rows: List[Dict[str, object]] = []
    promoted_ids: Set[str] = set()
    remaining_rows: List[Dict[str, str]] = []

    for row in unresolved_rows:
        cid = (row.get("combo_concept_id") or "").strip()
        combo_label = (row.get("combo_label") or "").strip()
        synonyms = (row.get("synonyms") or "").strip()
        parent = derive_parent_label(combo_label)
        if not parent:
            remaining_rows.append(row)
            continue
        parent_label, relation = parent
        parent_rec = parent_index.get(normalize_text(parent_label))
        if not parent_rec:
            remaining_rows.append(row)
            continue

        promoted_ids.add(cid)
        promoted_rows.append(
            {
                "input_curie": f"COMBO:{cid}",
                "preferred_identifier": parent_rec["preferred_identifier"],
                "preferred_label": parent_rec["preferred_label"],
                "confidence": 0.78 if relation == "narrow_to_parent" else 0.72,
                "method": "variant_parent_mapping",
                "mapping_relation": relation,
                "combo_concept_id": cid,
                "combo_label": combo_label,
                "source_identifier": f"COMBO:{cid}",
                "evidence": f"variant_parent:{relation}:{parent_label}",
            }
        )

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

    print(f"[ok] Unresolved input: {len(unresolved_rows)}")
    print(f"[ok] Variant-parent promoted: {len(promoted_rows)}")
    print(f"[ok] Unresolved remaining: {len(remaining_rows)}")
    print(f"[ok] Wrote: {args.promoted_output}")
    print(f"[ok] Wrote: {args.remaining_output}")


if __name__ == "__main__":
    main()
