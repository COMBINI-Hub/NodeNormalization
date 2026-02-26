#!/usr/bin/env python3
"""
Promote unresolved concepts by stripping lexical modifiers and matching to
already-mapped parent/core labels.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote mappings using modifier-stripping normalization.")
    parser.add_argument(
        "--unresolved-input",
        default="combo_unresolved_after_manual_curation.tsv",
        help="Input unresolved TSV.",
    )
    parser.add_argument(
        "--mapped-input",
        default="combo_seed_mappings.with_manual_curation.tsv",
        help="Mapped TSV used as target label index.",
    )
    parser.add_argument(
        "--promoted-output",
        default="combo_modifier_stripping_mappings.tsv",
        help="Output promoted mappings TSV.",
    )
    parser.add_argument(
        "--remaining-output",
        default="combo_unresolved_after_modifier.tsv",
        help="Remaining unresolved TSV.",
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


def build_label_index(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    idx: Dict[str, Dict[str, str]] = {}
    for row in rows:
        preferred_identifier = (row.get("preferred_identifier") or "").strip()
        if not preferred_identifier:
            continue
        labels = [
            normalize_text(row.get("combo_label") or ""),
            normalize_text(row.get("preferred_label") or ""),
        ]
        for label in labels:
            if not label:
                continue
            rec = {
                "preferred_identifier": preferred_identifier,
                "preferred_label": (row.get("preferred_label") or row.get("combo_label") or "").strip(),
                "confidence": str(row.get("confidence") or ""),
            }
            cur = idx.get(label)
            if cur is None:
                idx[label] = rec
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
                    idx[label] = rec
    return idx


def split_synonyms(raw: str) -> List[str]:
    out: List[str] = []
    for item in (raw or "").split("|"):
        item = item.strip()
        if not item or item.upper() == "N/A":
            continue
        out.append(item)
    return out


def stripped_forms(text: str) -> List[str]:
    t = normalize_text(text)
    if not t:
        return []
    forms: List[str] = []
    seen: Set[str] = set()

    def add(v: str) -> None:
        v = normalize_text(v)
        if not v or v in seen:
            return
        seen.add(v)
        forms.append(v)

    add(t)
    modifier_prefixes = [
        "mindfulness based ",
        "mindfulness ",
        "guided ",
        "group ",
        "individualized ",
        "individualised ",
        "active ",
        "interactive ",
        "instructional ",
        "improvisational ",
        "live ",
        "calming ",
        "assisted ",
        "medical ",
        "artistic ",
        "phenomenological ",
        "psychodynamic ",
        "anthroposophic ",
    ]
    suffixes = [
        " intervention",
        " training",
        " program",
        " protocol",
        " focused auditory therapy",
    ]

    for p in modifier_prefixes:
        if t.startswith(p):
            add(t[len(p) :])
    for s in suffixes:
        if t.endswith(s):
            add(t[: -len(s)])

    # Promote common anchor collapses.
    anchors = [
        "music therapy",
        "art therapy",
        "acupuncture",
        "moxibustion",
        "reflexology",
        "biofeedback",
        "yoga",
        "meditation",
        "qigong",
        "massage",
        "breathing exercise",
    ]
    for a in anchors:
        if a in t:
            add(a)

    return forms


def resolve_best_target(forms: List[str], idx: Dict[str, Dict[str, str]]) -> Optional[Tuple[str, Dict[str, str]]]:
    hits = [(f, idx[f]) for f in forms if f in idx]
    if not hits:
        return None
    # Prefer first generated form (most local) then stronger confidence.
    hits.sort(key=lambda x: forms.index(x[0]))
    return hits[0]


def main() -> None:
    args = parse_args()
    unresolved_rows = read_tsv(Path(args.unresolved_input))
    mapped_rows = read_tsv(Path(args.mapped_input))
    label_index = build_label_index(mapped_rows)

    promoted_rows: List[Dict[str, object]] = []
    promoted_ids: Set[str] = set()
    remaining_rows: List[Dict[str, str]] = []

    for row in unresolved_rows:
        cid = (row.get("combo_concept_id") or "").strip()
        combo_label = (row.get("combo_label") or "").strip()
        synonyms = (row.get("synonyms") or "").strip()
        tried_forms: List[str] = []
        for text in [combo_label, *split_synonyms(synonyms)]:
            tried_forms.extend(stripped_forms(text))
        # dedupe preserve order
        seen = set()
        forms = []
        for f in tried_forms:
            if f in seen:
                continue
            seen.add(f)
            forms.append(f)

        resolved = resolve_best_target(forms, label_index)
        if not resolved:
            remaining_rows.append(row)
            continue
        matched_form, target = resolved
        if matched_form == normalize_text(combo_label):
            # Exact label parity should have been handled earlier; keep for safety.
            remaining_rows.append(row)
            continue

        promoted_ids.add(cid)
        promoted_rows.append(
            {
                "input_curie": f"COMBO:{cid}",
                "preferred_identifier": target["preferred_identifier"],
                "preferred_label": target["preferred_label"],
                "confidence": 0.7,
                "method": "modifier_stripping_mapping",
                "mapping_relation": "close_match",
                "combo_concept_id": cid,
                "combo_label": combo_label,
                "source_identifier": f"COMBO:{cid}",
                "evidence": f"modifier_stripping:{matched_form}",
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
    print(f"[ok] Modifier-stripping promoted: {len(promoted_rows)}")
    print(f"[ok] Unresolved remaining: {len(remaining_rows)}")
    print(f"[ok] Wrote: {args.promoted_output}")
    print(f"[ok] Wrote: {args.remaining_output}")


if __name__ == "__main__":
    main()
