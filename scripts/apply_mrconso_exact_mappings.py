#!/usr/bin/env python3
"""
Apply conservative exact-string mappings from MRCONSO to unresolved COMBO concepts.

Outputs:
  - accepted mapping rows (pipeline-compatible)
  - review bucket for ambiguous matches
  - remaining unresolved concepts
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


@dataclass
class Candidate:
    preferred_identifier: str
    preferred_label: str
    sab: str
    code: str
    cui: str
    is_preferred: bool
    term_status_preferred: bool
    matched_query: str
    match_source: str  # label or synonym


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Map unresolved COMBO concepts using exact MRCONSO matches.")
    parser.add_argument(
        "--input-unresolved",
        default="combo_unresolved_concepts.tsv",
        help="Unresolved COMBO concepts TSV.",
    )
    parser.add_argument(
        "--mrconso",
        default="MRCONSO.RRF",
        help="Path to MRCONSO.RRF.",
    )
    parser.add_argument(
        "--accepted-output",
        default="combo_mrconso_exact_mappings.tsv",
        help="Accepted exact-match mappings TSV.",
    )
    parser.add_argument(
        "--review-output",
        default="combo_mrconso_exact_review.tsv",
        help="Ambiguous exact-match review TSV.",
    )
    parser.add_argument(
        "--remaining-output",
        default="combo_unresolved_after_mrconso.tsv",
        help="Still-unresolved concepts after exact-match stage.",
    )
    parser.add_argument(
        "--allowed-sabs",
        nargs="*",
        default=["MSH", "SNOMEDCT_US", "NCI", "RXNORM", "MTH"],
        help="MRCONSO source vocabularies to consider.",
    )
    parser.add_argument(
        "--max-candidates-per-concept",
        type=int,
        default=3,
        help="Accept automatically only when unique candidate IDs <= this count.",
    )
    parser.add_argument(
        "--min-token-count",
        type=int,
        default=2,
        help="Minimum tokens in concept label for automatic acceptance.",
    )
    parser.add_argument(
        "--min-char-length",
        type=int,
        default=8,
        help="Minimum normalized character length for automatic acceptance.",
    )
    parser.add_argument(
        "--ambiguous-short-token-blocklist",
        nargs="*",
        default=["cat", "st", "msc", "more", "raja", "haw"],
        help="Exact tokens always sent to review.",
    )
    return parser.parse_args()


def normalize_text(text: str) -> str:
    value = (text or "").lower().replace("_", " ").strip()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def token_count(text: str) -> int:
    norm = normalize_text(text)
    return len(norm.split()) if norm else 0


def split_synonyms(raw: str) -> List[str]:
    if not raw:
        return []
    out: List[str] = []
    for item in raw.split("|"):
        value = item.strip()
        if not value or value.upper() == "N/A":
            continue
        out.append(value)
    return out


def make_preferred_identifier(sab: str, code: str, cui: str) -> str:
    if sab == "MSH":
        return f"MESH:{code}"
    return f"UMLS:{cui}"


def sab_priority(sab: str) -> int:
    order = {
        "MSH": 0,
        "MTH": 1,
        "NCI": 2,
        "SNOMEDCT_US": 3,
        "RXNORM": 4,
    }
    return order.get(sab, 99)


def read_unresolved(path: Path) -> List[Dict[str, str]]:
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
    unresolved_rows = read_unresolved(Path(args.input_unresolved))
    allowed_sabs = set(args.allowed_sabs)
    ambiguous_blocklist = {normalize_text(x) for x in args.ambiguous_short_token_blocklist}

    # Build lookup from normalized query string -> concept references.
    query_to_concepts: Dict[str, List[Tuple[str, str, str, str]]] = defaultdict(list)
    for row in unresolved_rows:
        cid = (row.get("combo_concept_id") or "").strip()
        label = (row.get("combo_label") or "").strip()
        synonyms = split_synonyms(row.get("synonyms") or "")

        nlabel = normalize_text(label)
        if nlabel:
            query_to_concepts[nlabel].append((cid, label, label, "label"))
        for synonym in synonyms:
            ns = normalize_text(synonym)
            if ns:
                query_to_concepts[ns].append((cid, label, synonym, "synonym"))

    # Stream MRCONSO and collect exact matches.
    concept_candidates: Dict[str, Dict[str, Candidate]] = defaultdict(dict)
    with Path(args.mrconso).open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            parts = line.rstrip("\n").split("|")
            if len(parts) < 15:
                continue
            cui, lat, ts, _, _, _, ispref, _, _, _, _, sab, _, code, string = parts[:15]
            if lat != "ENG" or sab not in allowed_sabs:
                continue
            nstring = normalize_text(string)
            if not nstring or nstring not in query_to_concepts:
                continue

            preferred_identifier = make_preferred_identifier(sab, code, cui)
            for cid, concept_label, matched_query, match_source in query_to_concepts[nstring]:
                key = preferred_identifier
                # Keep best candidate per preferred identifier per concept.
                new_cand = Candidate(
                    preferred_identifier=preferred_identifier,
                    preferred_label=string.strip(),
                    sab=sab,
                    code=code,
                    cui=cui,
                    is_preferred=(ispref == "Y"),
                    term_status_preferred=(ts == "P"),
                    matched_query=matched_query,
                    match_source=match_source,
                )
                prev = concept_candidates[cid].get(key)
                if prev is None:
                    concept_candidates[cid][key] = new_cand
                else:
                    prev_rank = (sab_priority(prev.sab), 0 if prev.is_preferred else 1, 0 if prev.term_status_preferred else 1)
                    new_rank = (sab_priority(new_cand.sab), 0 if new_cand.is_preferred else 1, 0 if new_cand.term_status_preferred else 1)
                    if new_rank < prev_rank:
                        concept_candidates[cid][key] = new_cand

    accepted_rows: List[Dict[str, object]] = []
    review_rows: List[Dict[str, object]] = []
    remaining_rows: List[Dict[str, object]] = []

    for row in unresolved_rows:
        cid = (row.get("combo_concept_id") or "").strip()
        concept_label = (row.get("combo_label") or "").strip()
        synonyms = (row.get("synonyms") or "").strip()
        candidates = list(concept_candidates.get(cid, {}).values())

        if not candidates:
            remaining_rows.append(
                {
                    "combo_concept_id": cid,
                    "combo_label": concept_label,
                    "synonyms": synonyms,
                    "reason": "no_mrconso_exact_match",
                }
            )
            continue

        # Pick best candidate for accepted mapping row.
        candidates.sort(
            key=lambda c: (
                sab_priority(c.sab),
                0 if c.is_preferred else 1,
                0 if c.term_status_preferred else 1,
                c.preferred_identifier,
            )
        )
        best = candidates[0]

        normalized_label = normalize_text(concept_label)
        too_short = token_count(concept_label) < args.min_token_count and len(normalized_label) < args.min_char_length
        is_blocked = normalized_label in ambiguous_blocklist
        too_many = len(candidates) > args.max_candidates_per_concept

        if too_short or is_blocked or too_many:
            review_rows.append(
                {
                    "combo_concept_id": cid,
                    "combo_label": concept_label,
                    "synonyms": synonyms,
                    "candidate_count": len(candidates),
                    "best_identifier": best.preferred_identifier,
                    "best_label": best.preferred_label,
                    "best_sab": best.sab,
                    "matched_query": best.matched_query,
                    "match_source": best.match_source,
                    "reason": (
                        "ambiguous_blocklisted_token"
                        if is_blocked
                        else "ambiguous_many_candidates"
                        if too_many
                        else "short_term_requires_review"
                    ),
                    "candidates": "|".join(f"{c.preferred_identifier}::{c.preferred_label}::{c.sab}" for c in candidates[:20]),
                }
            )
            remaining_rows.append(
                {
                    "combo_concept_id": cid,
                    "combo_label": concept_label,
                    "synonyms": synonyms,
                    "reason": "mrconso_exact_review_required",
                }
            )
            continue

        accepted_rows.append(
            {
                "input_curie": f"COMBO:{cid}",
                "preferred_identifier": best.preferred_identifier,
                "preferred_label": best.preferred_label,
                "confidence": 0.97,
                "method": "mrconso_exact",
                "combo_concept_id": cid,
                "combo_label": concept_label,
                "source_identifier": f"COMBO:{cid}",
                "evidence": f"mrconso_exact:{best.sab}:{best.match_source}:{best.matched_query}",
            }
        )

    write_tsv(
        Path(args.accepted_output),
        accepted_rows,
        [
            "input_curie",
            "preferred_identifier",
            "preferred_label",
            "confidence",
            "method",
            "combo_concept_id",
            "combo_label",
            "source_identifier",
            "evidence",
        ],
    )
    write_tsv(
        Path(args.review_output),
        review_rows,
        [
            "combo_concept_id",
            "combo_label",
            "synonyms",
            "candidate_count",
            "best_identifier",
            "best_label",
            "best_sab",
            "matched_query",
            "match_source",
            "reason",
            "candidates",
        ],
    )
    write_tsv(
        Path(args.remaining_output),
        remaining_rows,
        ["combo_concept_id", "combo_label", "synonyms", "reason"],
    )

    print(f"[ok] Input unresolved concepts: {len(unresolved_rows)}")
    print(f"[ok] Concepts with exact MRCONSO candidates: {len(concept_candidates)}")
    print(f"[ok] Accepted exact mappings: {len(accepted_rows)}")
    print(f"[ok] Review bucket concepts: {len(review_rows)}")
    print(f"[ok] Remaining unresolved concepts: {len(remaining_rows)}")
    print(f"[ok] Wrote: {args.accepted_output}")
    print(f"[ok] Wrote: {args.review_output}")
    print(f"[ok] Wrote: {args.remaining_output}")


if __name__ == "__main__":
    main()
