#!/usr/bin/env python3
"""
Semantic fallback matcher for unresolved COMBO concepts.

This script performs lightweight semantic matching using normalized lexical
similarity (exact match, token Jaccard, sequence similarity) over candidate
identifier/label dictionaries from optional mapping files.
"""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply semantic fallback to unresolved COMBO concepts.")
    parser.add_argument(
        "--input-unresolved",
        required=True,
        help="Unresolved concept TSV (combo_concept_id, combo_label, synonyms, reason).",
    )
    parser.add_argument(
        "--base-mappings",
        default="combo_seed_mappings.tsv",
        help="Deterministic mapping TSV to extend.",
    )
    parser.add_argument(
        "--candidate-files",
        nargs="*",
        default=[],
        help="Optional candidate mapping files (CSV/TSV/JSON/JSONL) with identifier+label fields.",
    )
    parser.add_argument(
        "--semantic-output",
        default="combo_semantic_mappings.tsv",
        help="Rows accepted by semantic fallback.",
    )
    parser.add_argument(
        "--combined-output",
        default="combo_seed_mappings.with_semantic.tsv",
        help="Combined deterministic + semantic mapping output.",
    )
    parser.add_argument(
        "--remaining-output",
        default="combo_unresolved_after_semantic.tsv",
        help="Unresolved concepts after semantic fallback.",
    )
    parser.add_argument("--threshold", type=float, default=0.86, help="Minimum best-score to accept a semantic match.")
    parser.add_argument(
        "--margin",
        type=float,
        default=0.06,
        help="Minimum score gap between top-1 and top-2 candidates.",
    )
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=250000,
        help="Maximum candidate rows to load (safety guard).",
    )
    return parser.parse_args()


@dataclass
class Candidate:
    identifier: str
    label: str
    source_file: str


def read_tsv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        return [dict(row) for row in reader]


def read_delimited(path: Path, delimiter: str) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        return [dict(row) for row in reader]


def write_tsv(path: Path, rows: Iterable[Dict[str, object]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def normalize_text(text: str) -> str:
    value = (text or "").lower().strip()
    value = value.replace("_", " ")
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def token_set(text: str) -> set[str]:
    if not text:
        return set()
    return set(text.split())


def score_similarity(query: str, candidate: str) -> float:
    qn = normalize_text(query)
    cn = normalize_text(candidate)
    if not qn or not cn:
        return 0.0
    if qn == cn:
        return 1.0

    qtok = token_set(qn)
    ctok = token_set(cn)
    jaccard = (len(qtok & ctok) / len(qtok | ctok)) if qtok and ctok else 0.0
    ratio = difflib.SequenceMatcher(a=qn, b=cn).ratio()
    return max(jaccard, ratio)


def parse_candidate_row(row: Dict[str, object], source_file: str) -> Optional[Candidate]:
    identifier = None
    label = None
    for key in ("preferred_identifier", "canonical_id", "normalized_identifier", "identifier", "id", "curie"):
        value = row.get(key)
        if value:
            identifier = str(value).strip()
            break
    for key in ("preferred_label", "label", "name", "node_label"):
        value = row.get(key)
        if value:
            label = str(value).strip()
            break
    if not identifier or not label:
        return None
    return Candidate(identifier=identifier, label=label, source_file=source_file)


def load_candidates(paths: Sequence[str], max_candidates: int) -> List[Candidate]:
    candidates: List[Candidate] = []
    seen = set()

    for raw in paths:
        path = Path(raw)
        if not path.exists():
            print(f"[warn] Candidate file missing, skipping: {path}")
            continue
        suffix = path.suffix.lower()
        rows: List[Dict[str, object]] = []
        if suffix == ".tsv":
            rows = read_delimited(path, "\t")
        elif suffix == ".csv":
            rows = read_delimited(path, ",")
        elif suffix == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                rows = [x for x in payload if isinstance(x, dict)]
            elif isinstance(payload, dict):
                rows = [payload]
        elif suffix == ".jsonl":
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(obj, dict):
                        rows.append(obj)
        else:
            print(f"[warn] Unsupported candidate file type, skipping: {path}")
            continue

        for row in rows:
            cand = parse_candidate_row(row, str(path))
            if not cand:
                continue
            key = (cand.identifier, normalize_text(cand.label))
            if key in seen:
                continue
            seen.add(key)
            candidates.append(cand)
            if len(candidates) >= max_candidates:
                print(f"[warn] Reached max candidates limit ({max_candidates}), truncating load.")
                return candidates

    return candidates


def get_aliases(label: str, synonyms_pipe: str) -> List[str]:
    aliases = [label or ""]
    if synonyms_pipe:
        aliases.extend([s.strip() for s in synonyms_pipe.split("|") if s.strip()])
    out: List[str] = []
    seen = set()
    for alias in aliases:
        norm = normalize_text(alias)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(alias)
    return out


def best_two_matches(aliases: Sequence[str], candidates: Sequence[Candidate]) -> Tuple[Tuple[Optional[Candidate], float], float]:
    best_candidate: Optional[Candidate] = None
    best_score = 0.0
    second_score = 0.0

    for cand in candidates:
        cand_score = 0.0
        for alias in aliases:
            cand_score = max(cand_score, score_similarity(alias, cand.label))
            if cand_score == 1.0:
                break
        if cand_score > best_score:
            second_score = best_score
            best_score = cand_score
            best_candidate = cand
        elif cand_score > second_score:
            second_score = cand_score

    return (best_candidate, best_score), second_score


def main() -> None:
    args = parse_args()
    unresolved_rows = read_tsv(Path(args.input_unresolved))
    base_rows = read_tsv(Path(args.base_mappings))
    candidates = load_candidates(args.candidate_files, args.max_candidates)

    semantic_rows: List[Dict[str, object]] = []
    remaining_rows: List[Dict[str, object]] = []

    if not candidates:
        print("[warn] No candidate dictionary provided; semantic fallback skipped.")
        write_tsv(
            Path(args.semantic_output),
            semantic_rows,
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
            Path(args.combined_output),
            base_rows,
            list(base_rows[0].keys()) if base_rows else [
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
        write_tsv(Path(args.remaining_output), unresolved_rows, ["combo_concept_id", "combo_label", "synonyms", "reason"])
        print(f"[ok] Wrote: {args.semantic_output}")
        print(f"[ok] Wrote: {args.combined_output}")
        print(f"[ok] Wrote: {args.remaining_output}")
        return

    for row in unresolved_rows:
        combo_id = (row.get("combo_concept_id") or "").strip()
        combo_label = (row.get("combo_label") or "").strip()
        synonyms = (row.get("synonyms") or "").strip()
        aliases = get_aliases(combo_label, synonyms)

        (best_candidate, best_score), second_score = best_two_matches(aliases, candidates)
        margin = best_score - second_score

        if best_candidate and best_score >= args.threshold and margin >= args.margin:
            semantic_rows.append(
                {
                    "input_curie": f"COMBO:{combo_id}",
                    "preferred_identifier": best_candidate.identifier,
                    "preferred_label": best_candidate.label,
                    "confidence": round(best_score, 4),
                    "method": "semantic_fallback",
                    "combo_concept_id": combo_id,
                    "combo_label": combo_label,
                    "source_identifier": f"COMBO:{combo_id}",
                    "evidence": f"semantic_label_match:{best_candidate.source_file}",
                }
            )
        else:
            remaining_rows.append(
                {
                    "combo_concept_id": combo_id,
                    "combo_label": combo_label,
                    "synonyms": synonyms,
                    "reason": "semantic_threshold_not_met",
                }
            )

    combined_rows = list(base_rows) + semantic_rows
    output_fields = (
        list(base_rows[0].keys())
        if base_rows
        else [
            "input_curie",
            "preferred_identifier",
            "preferred_label",
            "confidence",
            "method",
            "combo_concept_id",
            "combo_label",
            "source_identifier",
            "evidence",
        ]
    )

    write_tsv(Path(args.semantic_output), semantic_rows, output_fields)
    write_tsv(Path(args.combined_output), combined_rows, output_fields)
    write_tsv(Path(args.remaining_output), remaining_rows, ["combo_concept_id", "combo_label", "synonyms", "reason"])

    print(f"[ok] Unresolved concepts (input): {len(unresolved_rows)}")
    print(f"[ok] Semantic mappings accepted: {len(semantic_rows)}")
    print(f"[ok] Remaining unresolved: {len(remaining_rows)}")
    print(f"[ok] Wrote: {args.semantic_output}")
    print(f"[ok] Wrote: {args.combined_output}")
    print(f"[ok] Wrote: {args.remaining_output}")


if __name__ == "__main__":
    main()
