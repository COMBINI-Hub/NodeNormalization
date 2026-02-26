#!/usr/bin/env python3
"""
Apply curated CAM lexicon mappings for unresolved concepts.

Lexicon rows map variant text -> parent label + non-exact relation.
The parent label is resolved to a preferred identifier using existing mapped rows.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply curated CAM lexicon mappings.")
    parser.add_argument(
        "--unresolved-input",
        default="combo_unresolved_after_variant_parent.tsv",
        help="Unresolved concepts TSV input.",
    )
    parser.add_argument(
        "--mapped-input",
        default="combo_seed_mappings.with_mrconso_exact_and_review.tsv",
        help="Mapped concepts TSV used to resolve parent labels.",
    )
    parser.add_argument(
        "--lexicon",
        default="scripts/cam_lexicon_seed.tsv",
        help="CAM lexicon TSV mapping variant_text -> parent_label.",
    )
    parser.add_argument(
        "--promoted-output",
        default="combo_cam_lexicon_mappings.tsv",
        help="Output promoted mapping rows.",
    )
    parser.add_argument(
        "--remaining-output",
        default="combo_unresolved_after_cam_lexicon.tsv",
        help="Remaining unresolved rows after lexicon stage.",
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
    index: Dict[str, Dict[str, str]] = {}
    for row in rows:
        key = normalize_text(row.get("combo_label") or row.get("preferred_label") or "")
        if not key:
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
        cur = index.get(key)
        if cur is None:
            index[key] = rec
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
                index[key] = rec
    return index


def build_lexicon(path: Path) -> Dict[str, Tuple[str, str]]:
    lex: Dict[str, Tuple[str, str]] = {}
    for row in read_tsv(path):
        variant = normalize_text(row.get("variant_text") or "")
        parent = normalize_text(row.get("parent_label") or "")
        relation = (row.get("mapping_relation") or "close_match").strip()
        if not variant or not parent:
            continue
        lex[variant] = (parent, relation)
    return lex


def concept_aliases(row: Dict[str, str]) -> List[str]:
    aliases = [(row.get("combo_label") or "").strip()]
    aliases.extend(
        [s.strip() for s in (row.get("synonyms") or "").split("|") if s.strip() and s.strip().upper() != "N/A"]
    )
    out = []
    seen = set()
    for alias in aliases:
        n = normalize_text(alias)
        if not n or n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def main() -> None:
    args = parse_args()
    unresolved_rows = read_tsv(Path(args.unresolved_input))
    mapped_rows = read_tsv(Path(args.mapped_input))
    parent_index = build_parent_index(mapped_rows)
    lexicon = build_lexicon(Path(args.lexicon))

    promoted_rows: List[Dict[str, object]] = []
    remaining_rows: List[Dict[str, str]] = []
    promoted_ids: Set[str] = set()

    for row in unresolved_rows:
        cid = (row.get("combo_concept_id") or "").strip()
        combo_label = (row.get("combo_label") or "").strip()
        synonyms = (row.get("synonyms") or "").strip()
        matched = None
        for alias in concept_aliases(row):
            if alias in lexicon:
                matched = (alias, *lexicon[alias])
                break

        if not matched:
            remaining_rows.append(row)
            continue

        matched_alias, parent_label, relation = matched
        parent_rec = parent_index.get(parent_label)
        if not parent_rec:
            remaining_rows.append(row)
            continue

        promoted_ids.add(cid)
        promoted_rows.append(
            {
                "input_curie": f"COMBO:{cid}",
                "preferred_identifier": parent_rec["preferred_identifier"],
                "preferred_label": parent_rec["preferred_label"],
                "confidence": 0.8 if relation == "narrow_to_parent" else 0.74,
                "method": "cam_lexicon_mapping",
                "mapping_relation": relation,
                "combo_concept_id": cid,
                "combo_label": combo_label,
                "source_identifier": f"COMBO:{cid}",
                "evidence": f"cam_lexicon:{matched_alias}:{parent_label}:{relation}",
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
    print(f"[ok] CAM lexicon promoted: {len(promoted_rows)}")
    print(f"[ok] Unresolved remaining: {len(remaining_rows)}")
    print(f"[ok] Wrote: {args.promoted_output}")
    print(f"[ok] Wrote: {args.remaining_output}")


if __name__ == "__main__":
    main()
