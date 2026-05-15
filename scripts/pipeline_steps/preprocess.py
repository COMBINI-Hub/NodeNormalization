"""
Step 1 — preprocess_raw_edges

Reads raw iKraph and SemMedDB edge files, removes self-loops, deduplicates on
(source, target, predicate), and writes cleaned CSVs for downstream steps.

Outputs (in norm_dir):
  ikraph_edges_cleaned.csv   — from DBRelations.json; direction field preserved
  semmeddb_edges_cleaned.csv — from connections.csv
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


def preprocess_ikraph_edges(db_relations_json: Path, output_path: Path) -> int:
    """
    Read DBRelations.json, remove self-loops, deduplicate on
    (node_one_id, node_two_id, relationship_type), and write a CSV.

    The direction field is preserved as-is; the edge-building step applies
    the swap when direction == "21".

    Returns the number of edges written.
    """
    print(f"[preprocess] loading {db_relations_json.name}...", file=sys.stderr)
    with db_relations_json.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    seen: set = set()
    written = 0
    skipped_self = 0
    skipped_dupe = 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(
            [
                "node_one_id", "node_two_id", "relationship_type",
                "direction", "source", "correlation_type",
                "node_one_type", "node_two_type",
                "node_one_name", "node_two_name",
                "prob", "method", "score", "relID",
            ]
        )
        for rec in data:
            n1 = str(rec.get("node_one_id") or "").strip()
            n2 = str(rec.get("node_two_id") or "").strip()
            rel = str(rec.get("relationship_type") or "").strip()
            if not n1 or not n2 or not rel:
                continue
            if n1 == n2:
                skipped_self += 1
                continue
            key = (n1, n2, rel)
            if key in seen:
                skipped_dupe += 1
                continue
            seen.add(key)
            writer.writerow([
                n1, n2, rel,
                rec.get("direction", "0"),
                rec.get("source", ""),
                rec.get("correlation_type", ""),
                rec.get("node_one_type", ""),
                rec.get("node_two_type", ""),
                rec.get("node_one_name", ""),
                rec.get("node_two_name", ""),
                rec.get("prob", ""),
                rec.get("method", ""),
                rec.get("score", ""),
                rec.get("relID", ""),
            ])
            written += 1

    print(
        f"[preprocess] iKraph: {written:,} edges written "
        f"({skipped_self:,} self-loops, {skipped_dupe:,} duplicates removed)",
        file=sys.stderr,
    )
    return written


def preprocess_semmed_edges(connections_csv: Path, output_path: Path) -> int:
    """
    Read SemMedDB connections.csv (headerless: col0=source, col1=target, col2=predicate),
    remove self-loops, deduplicate on (source, target, predicate).

    Returns the number of edges written.
    """
    print(f"[preprocess] loading {connections_csv.name}...", file=sys.stderr)
    seen: set = set()
    written = 0
    skipped_self = 0
    skipped_dupe = 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with (
        connections_csv.open("r", encoding="utf-8", errors="replace") as infile,
        output_path.open("w", encoding="utf-8", newline="") as out,
    ):
        writer = csv.writer(out)
        writer.writerow([":START_ID", ":END_ID", ":TYPE", "frequency%"])
        for line in infile:
            parts = line.rstrip().split(",")
            if len(parts) < 3:
                continue
            src, tgt, pred = parts[0].strip(), parts[1].strip(), parts[2].strip()
            if not src or not tgt or not pred:
                continue
            if src == tgt:
                skipped_self += 1
                continue
            key = (src, tgt, pred)
            if key in seen:
                skipped_dupe += 1
                continue
            seen.add(key)
            row = parts + [""] * (4 - len(parts))
            writer.writerow(row[:4])
            written += 1

    print(
        f"[preprocess] SemMed: {written:,} edges written "
        f"({skipped_self:,} self-loops, {skipped_dupe:,} duplicates removed)",
        file=sys.stderr,
    )
    return written
