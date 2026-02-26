#!/usr/bin/env python3
"""
Promote unresolved concepts by backing off to nearest mapped COMBO ancestor.

Mappings emitted are non-exact and explicitly annotated as narrow_to_parent.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote unresolved concepts to nearest mapped ancestor.")
    parser.add_argument(
        "--unresolved-input",
        default="combo_unresolved_after_cam_lexicon.tsv",
        help="Input unresolved TSV.",
    )
    parser.add_argument(
        "--mapped-input",
        default="combo_seed_mappings.with_cam_lexicon.tsv",
        help="Mapped TSV used to resolve ancestor identifiers.",
    )
    parser.add_argument(
        "--promoted-output",
        default="combo_hierarchy_backoff_mappings.tsv",
        help="Output promoted mappings TSV.",
    )
    parser.add_argument(
        "--remaining-output",
        default="combo_unresolved_after_hierarchy_backoff.tsv",
        help="Remaining unresolved TSV.",
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


def build_mapped_by_cid(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        cid = (row.get("combo_concept_id") or "").strip()
        preferred_identifier = (row.get("preferred_identifier") or "").strip()
        if not cid or not preferred_identifier:
            continue
        rec = {
            "preferred_identifier": preferred_identifier,
            "preferred_label": (row.get("preferred_label") or "").strip(),
            "confidence": str(row.get("confidence") or ""),
        }
        cur = out.get(cid)
        if cur is None:
            out[cid] = rec
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
                out[cid] = rec
    return out


def nearest_mapped_ancestor(cid: str, mapped: Dict[str, Dict[str, str]]) -> Optional[Tuple[str, Dict[str, str], int]]:
    parts = cid.split(".")
    distance = 0
    while len(parts) > 1:
        parts = parts[:-1]
        distance += 1
        parent = ".".join(parts)
        if parent in mapped:
            return parent, mapped[parent], distance
    return None


def confidence_for_distance(distance: int) -> float:
    # Conservative decay for deeper backoff.
    if distance <= 1:
        return 0.72
    if distance == 2:
        return 0.66
    if distance == 3:
        return 0.6
    return 0.55


def main() -> None:
    args = parse_args()
    unresolved_rows = read_tsv(Path(args.unresolved_input))
    mapped_rows = read_tsv(Path(args.mapped_input))
    mapped_by_cid = build_mapped_by_cid(mapped_rows)

    promoted_rows: List[Dict[str, object]] = []
    promoted_ids: Set[str] = set()
    remaining_rows: List[Dict[str, str]] = []

    for row in unresolved_rows:
        cid = (row.get("combo_concept_id") or "").strip()
        combo_label = (row.get("combo_label") or "").strip()
        synonyms = (row.get("synonyms") or "").strip()
        anc = nearest_mapped_ancestor(cid, mapped_by_cid)
        if not anc:
            remaining_rows.append(row)
            continue
        ancestor_cid, ancestor_rec, distance = anc
        promoted_ids.add(cid)
        promoted_rows.append(
            {
                "input_curie": f"COMBO:{cid}",
                "preferred_identifier": ancestor_rec["preferred_identifier"],
                "preferred_label": ancestor_rec["preferred_label"],
                "confidence": confidence_for_distance(distance),
                "method": "hierarchy_backoff_mapping",
                "mapping_relation": "narrow_to_parent",
                "combo_concept_id": cid,
                "combo_label": combo_label,
                "source_identifier": f"COMBO:{cid}",
                "evidence": f"hierarchy_backoff:ancestor={ancestor_cid}:distance={distance}",
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
    print(f"[ok] Hierarchy-backoff promoted: {len(promoted_rows)}")
    print(f"[ok] Unresolved remaining: {len(remaining_rows)}")
    print(f"[ok] Wrote: {args.promoted_output}")
    print(f"[ok] Wrote: {args.remaining_output}")


if __name__ == "__main__":
    main()
