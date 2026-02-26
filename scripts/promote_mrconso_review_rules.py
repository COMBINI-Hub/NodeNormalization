#!/usr/bin/env python3
"""
Promote safe rows from combo_mrconso_exact_review.tsv into accepted mappings.

This stage applies conservative rules to auto-accept review rows that are
high-confidence label-level MeSH matches, while leaving ambiguous rows for
manual curation.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote safe MRCONSO review rows with conservative rules.")
    parser.add_argument(
        "--review-input",
        default="combo_mrconso_exact_review.tsv",
        help="Input MRCONSO review TSV.",
    )
    parser.add_argument(
        "--unresolved-input",
        default="combo_unresolved_after_mrconso.tsv",
        help="Input unresolved TSV after exact MRCONSO stage.",
    )
    parser.add_argument(
        "--promoted-output",
        default="combo_mrconso_review_promoted.tsv",
        help="Output promoted mappings TSV.",
    )
    parser.add_argument(
        "--remaining-review-output",
        default="combo_mrconso_exact_review.remaining.tsv",
        help="Remaining review rows after promotion.",
    )
    parser.add_argument(
        "--unresolved-output",
        default="combo_unresolved_after_mrconso_review.tsv",
        help="Unresolved TSV after review-rule promotion.",
    )
    parser.add_argument(
        "--max-candidate-count",
        type=int,
        default=4,
        help="Promote only when candidate_count is <= this threshold.",
    )
    parser.add_argument(
        "--min-label-char-length",
        type=int,
        default=8,
        help="Promote only when concept label normalized length is >= this value.",
    )
    parser.add_argument(
        "--blocked-label-tokens",
        nargs="*",
        default=["cat", "st", "msc", "more", "raja", "haw", "tea"],
        help="Labels matching these normalized tokens are never auto-promoted.",
    )
    return parser.parse_args()


def normalize_text(text: str) -> str:
    value = (text or "").lower().replace("_", " ").strip()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def token_set(text: str) -> Set[str]:
    norm = normalize_text(text)
    return set(norm.split()) if norm else set()


def score_similarity(left: str, right: str) -> float:
    ln = normalize_text(left)
    rn = normalize_text(right)
    if not ln or not rn:
        return 0.0
    if ln == rn:
        return 1.0
    lt = token_set(ln)
    rt = token_set(rn)
    jaccard = (len(lt & rt) / len(lt | rt)) if lt and rt else 0.0
    overlap = (len(lt & rt) / len(lt)) if lt else 0.0
    # Weighted blend: exact lexical overlap + containment signal.
    return max(jaccard, overlap)


def parse_candidates(raw: str) -> List[Tuple[str, str, str]]:
    out: List[Tuple[str, str, str]] = []
    for item in (raw or "").split("|"):
        item = item.strip()
        if not item:
            continue
        parts = item.split("::")
        if len(parts) != 3:
            continue
        out.append((parts[0].strip(), parts[1].strip(), parts[2].strip()))
    return out


def pick_best_mesh_candidate(row: Dict[str, str], use_synonyms: bool = False) -> Optional[Tuple[str, str, float, float]]:
    aliases = [(row.get("combo_label") or "").strip()]
    if use_synonyms:
        aliases.extend(
            [s.strip() for s in (row.get("synonyms") or "").split("|") if s.strip() and s.strip().upper() != "N/A"]
        )
    aliases = [a for a in aliases if a]
    if not aliases:
        return None

    mesh_scored: List[Tuple[str, str, float]] = []
    for identifier, label, sab in parse_candidates(row.get("candidates") or ""):
        if sab != "MSH" or not identifier.startswith("MESH:"):
            continue
        score = max(score_similarity(alias, label) for alias in aliases)
        mesh_scored.append((identifier, label, score))

    if not mesh_scored:
        return None
    mesh_scored.sort(key=lambda x: (-x[2], x[0]))
    best_id, best_label, best_score = mesh_scored[0]
    second_score = mesh_scored[1][2] if len(mesh_scored) > 1 else 0.0
    margin = best_score - second_score
    return best_id, best_label, best_score, margin


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


def should_promote(row: Dict[str, str], blocked: Set[str], max_candidate_count: int, min_label_char_length: int) -> bool:
    combo_label = (row.get("combo_label") or "").strip()
    best_identifier = (row.get("best_identifier") or "").strip()
    best_sab = (row.get("best_sab") or "").strip()
    match_source = (row.get("match_source") or "").strip()
    candidate_count_raw = (row.get("candidate_count") or "").strip()

    try:
        candidate_count = int(candidate_count_raw)
    except ValueError:
        return False

    normalized_label = normalize_text(combo_label)
    if not normalized_label:
        return False
    if normalized_label in blocked:
        return False
    if len(normalized_label) < min_label_char_length:
        return False
    if candidate_count > max_candidate_count:
        return False

    # Conservative acceptance: label-level MeSH match only.
    if match_source != "label":
        return False
    if best_sab != "MSH":
        return False
    if not best_identifier.startswith("MESH:"):
        return False

    return True


def main() -> None:
    args = parse_args()
    blocked = {normalize_text(x) for x in args.blocked_label_tokens}

    review_rows = read_tsv(Path(args.review_input))
    unresolved_rows = read_tsv(Path(args.unresolved_input))

    promoted_rows: List[Dict[str, object]] = []
    remaining_review_rows: List[Dict[str, str]] = []
    promoted_ids: Set[str] = set()

    for row in review_rows:
        cid = (row.get("combo_concept_id") or "").strip()
        if should_promote(row, blocked, args.max_candidate_count, args.min_label_char_length):
            promoted_ids.add(cid)
            promoted_rows.append(
                {
                    "input_curie": f"COMBO:{cid}",
                    "preferred_identifier": (row.get("best_identifier") or "").strip(),
                    "preferred_label": (row.get("best_label") or "").strip(),
                    "confidence": 0.9,
                    "method": "mrconso_review_rule",
                    "combo_concept_id": cid,
                    "combo_label": (row.get("combo_label") or "").strip(),
                    "source_identifier": f"COMBO:{cid}",
                    "evidence": (
                        f"mrconso_review_rule:{(row.get('best_sab') or '').strip()}:"
                        f"{(row.get('match_source') or '').strip()}:{(row.get('matched_query') or '').strip()}"
                    ),
                }
            )
            continue

        # Botanical-specific conservative rule:
        # For herb/supplement branch rows, choose the best-scoring MeSH candidate
        # based on combo label/synonyms and promote only with strong separation.
        if cid.startswith("2.2.2."):
            normalized_label = normalize_text(row.get("combo_label") or "")
            if normalized_label in blocked:
                pick = None
            else:
                # Conservative: score botanical candidates against combo label only
                # to avoid synonym-driven drift (e.g., unrelated common names).
                pick = pick_best_mesh_candidate(row, use_synonyms=False)
            if pick is not None:
                best_id, best_label, best_score, margin = pick
                if best_score >= 0.9 and margin >= 0.1:
                    promoted_ids.add(cid)
                    promoted_rows.append(
                        {
                            "input_curie": f"COMBO:{cid}",
                            "preferred_identifier": best_id,
                            "preferred_label": best_label,
                            "confidence": round(0.85 + 0.1 * best_score, 4),
                            "method": "mrconso_review_rule_botanical",
                            "combo_concept_id": cid,
                            "combo_label": (row.get("combo_label") or "").strip(),
                            "source_identifier": f"COMBO:{cid}",
                            "evidence": (
                                "mrconso_review_rule_botanical:"
                                f"mesh_score={best_score:.3f}:margin={margin:.3f}"
                            ),
                        }
                    )
                    continue

        if cid not in promoted_ids:
            remaining_review_rows.append(row)

    remaining_unresolved = [
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
            "combo_concept_id",
            "combo_label",
            "source_identifier",
            "evidence",
        ],
    )
    write_tsv(
        Path(args.remaining_review_output),
        remaining_review_rows,
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
        Path(args.unresolved_output),
        remaining_unresolved,
        ["combo_concept_id", "combo_label", "synonyms", "reason"],
    )

    print(f"[ok] Review rows input: {len(review_rows)}")
    print(f"[ok] Promoted rows: {len(promoted_rows)}")
    print(f"[ok] Review rows remaining: {len(remaining_review_rows)}")
    print(f"[ok] Unresolved input: {len(unresolved_rows)}")
    print(f"[ok] Unresolved remaining after promotion: {len(remaining_unresolved)}")
    print(f"[ok] Wrote: {args.promoted_output}")
    print(f"[ok] Wrote: {args.remaining_review_output}")
    print(f"[ok] Wrote: {args.unresolved_output}")


if __name__ == "__main__":
    main()
