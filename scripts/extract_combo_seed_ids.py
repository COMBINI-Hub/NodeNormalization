#!/usr/bin/env python3
"""
Extract COMBO concept metadata and deterministic seed IDs from an RTF export.

The parser is intentionally pattern-based (not full RTF parsing) and targets:
- Concept headings keyed by numeric hierarchy IDs (e.g. 1.1.1.1)
- Provenance links for UMLS and MeSH
- Synonym bullets in concept sections
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


BOOKMARK_RE = re.compile(r"\\\*\\bkmkstart\s+((\d+(?:\.\d+)+)_[^\s}]+)")
HEADER_RE_TEMPLATE = r"\b{concept_id}\s+([A-Za-z][A-Za-z0-9_]+)"
UMLS_URL_RE = re.compile(r"https?://uts\.nlm\.nih\.gov/uts/umls/concept/(C\d+)", re.IGNORECASE)
MESH_URL_RE = re.compile(r"https?://purl\.bioontology\.org/ontology/MESH/([A-Z]\d+)", re.IGNORECASE)
GENERIC_UMSL_RE = re.compile(r"\bC\d{7,8}\b")
GENERIC_MESH_RE = re.compile(r"\bD\d{6}\b")
SYNONYMS_BLOCK_RE = re.compile(r"Synonyms:(.*?)(?:\\pard\\plain|\\\*\\bkmkstart|$)", re.IGNORECASE | re.DOTALL)
BULLET_RE = re.compile(r"\\bullet\s+([^\\{}]+)")


@dataclass
class ComboConcept:
    combo_concept_id: str
    combo_label: str
    umls_ids: List[str]
    mesh_ids: List[str]
    synonyms: List[str]
    provenance_urls: List[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract COMBO provenance seed IDs from an RTF file.")
    parser.add_argument("--input", required=True, help="Path to COMBO RTF document.")
    parser.add_argument(
        "--output",
        default="combo_concepts_raw.tsv",
        help="Output TSV for extracted concept metadata.",
    )
    parser.add_argument(
        "--unresolved-output",
        default="combo_unresolved_concepts.tsv",
        help="Output TSV for concepts without deterministic UMLS/MeSH IDs.",
    )
    return parser.parse_args()


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for raw in values:
        value = (raw or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _extract_heading_label(section_text: str, concept_id: str) -> str:
    header_re = re.compile(HEADER_RE_TEMPLATE.format(concept_id=re.escape(concept_id)))
    match = header_re.search(section_text)
    if match:
        return match.group(1).strip()
    return f"Concept_{concept_id}"


def _extract_synonyms(section_text: str) -> List[str]:
    match = SYNONYMS_BLOCK_RE.search(section_text)
    if not match:
        return []
    syn_block = match.group(1)
    raw_synonyms = BULLET_RE.findall(syn_block)
    cleaned = []
    for candidate in raw_synonyms:
        value = re.sub(r"\s+", " ", candidate).strip()
        if not value:
            continue
        if value.lower().startswith("source:"):
            continue
        if "http://" in value.lower() or "https://" in value.lower():
            continue
        cleaned.append(value)
    return _dedupe_keep_order(cleaned)


def _extract_provenance_ids(section_text: str) -> tuple[List[str], List[str], List[str]]:
    umls_hits = [f"UMLS:{cui.upper()}" for cui in UMLS_URL_RE.findall(section_text)]
    mesh_hits = [f"MESH:{mid.upper()}" for mid in MESH_URL_RE.findall(section_text)]

    # Fallback if links are malformed/missing but IDs are present in text.
    if not umls_hits:
        umls_hits = [f"UMLS:{cui.upper()}" for cui in GENERIC_UMSL_RE.findall(section_text)]
    if not mesh_hits:
        mesh_hits = [f"MESH:{mid.upper()}" for mid in GENERIC_MESH_RE.findall(section_text)]

    provenance_urls = _dedupe_keep_order(
        re.findall(r"https?://[^\s\\}]+", section_text, flags=re.IGNORECASE)
    )
    return _dedupe_keep_order(umls_hits), _dedupe_keep_order(mesh_hits), provenance_urls


def parse_combo_rtf(text: str) -> List[ComboConcept]:
    bookmarks = [(m.start(), m.group(2)) for m in BOOKMARK_RE.finditer(text)]
    if not bookmarks:
        return []

    concepts: List[ComboConcept] = []
    seen_ids = set()

    for idx, (start, concept_id) in enumerate(bookmarks):
        # Ignore duplicate section appearances in multi-inheritance references.
        if concept_id in seen_ids:
            continue
        end = bookmarks[idx + 1][0] if idx + 1 < len(bookmarks) else len(text)
        section = text[start:end]

        label = _extract_heading_label(section, concept_id)
        umls_ids, mesh_ids, provenance_urls = _extract_provenance_ids(section)
        synonyms = _extract_synonyms(section)

        concepts.append(
            ComboConcept(
                combo_concept_id=concept_id,
                combo_label=label,
                umls_ids=umls_ids,
                mesh_ids=mesh_ids,
                synonyms=synonyms,
                provenance_urls=provenance_urls,
            )
        )
        seen_ids.add(concept_id)

    concepts.sort(key=lambda row: tuple(int(part) for part in row.combo_concept_id.split(".")))
    return concepts


def write_tsv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    text = input_path.read_text(encoding="utf-8", errors="ignore")

    concepts = parse_combo_rtf(text)
    rows = []
    unresolved_rows = []

    for concept in concepts:
        row = {
            "combo_concept_id": concept.combo_concept_id,
            "combo_label": concept.combo_label,
            "umls_ids": "|".join(concept.umls_ids),
            "mesh_ids": "|".join(concept.mesh_ids),
            "synonyms": "|".join(concept.synonyms),
            "provenance_urls": "|".join(concept.provenance_urls),
        }
        rows.append(row)
        if not concept.umls_ids and not concept.mesh_ids:
            unresolved_rows.append(
                {
                    "combo_concept_id": concept.combo_concept_id,
                    "combo_label": concept.combo_label,
                    "synonyms": "|".join(concept.synonyms),
                    "reason": "no_deterministic_umls_or_mesh",
                }
            )

    write_tsv(
        Path(args.output),
        rows,
        ["combo_concept_id", "combo_label", "umls_ids", "mesh_ids", "synonyms", "provenance_urls"],
    )
    write_tsv(
        Path(args.unresolved_output),
        unresolved_rows,
        ["combo_concept_id", "combo_label", "synonyms", "reason"],
    )

    mapped = len(rows) - len(unresolved_rows)
    print(f"[ok] Parsed concepts: {len(rows)}")
    print(f"[ok] Concepts with deterministic IDs: {mapped}")
    print(f"[ok] Concepts unresolved: {len(unresolved_rows)}")
    print(f"[ok] Wrote: {args.output}")
    print(f"[ok] Wrote: {args.unresolved_output}")


if __name__ == "__main__":
    main()
