"""
Step 1 — preprocess_raw_edges

Reads raw iKraph and SemMedDB edge files, removes self-loops, deduplicates on
(source, target, predicate), and writes cleaned CSVs for downstream steps.

Memory profile: Uses **streaming** (ijson where available) and **SQLite-backed
deduplication** so we never hold full DBRelations.json / connections.csv in RAM.

Outputs (in norm_dir):
  ikraph_edges_cleaned.csv   — from DBRelations.json; direction field preserved
  semmeddb_edges_cleaned.csv — from connections.csv
"""
from __future__ import annotations

import csv
import json
import sqlite3
import sys
from pathlib import Path


def _init_sqlite(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-200000")
    return conn


def _iter_db_relations_records(db_relations_json: Path):
    """Stream dict records from DBRelations.json (top-level JSON array)."""
    try:
        import ijson  # type: ignore[import-untyped]

        with db_relations_json.open("rb") as fh:
            yield from ijson.items(fh, "item")
    except ImportError:
        print(
            "[preprocess] WARNING: `ijson` not installed — loading full DBRelations.json into RAM."
            "  Install `ijson` for low-memory preprocessing: pip install ijson",
            file=sys.stderr,
        )
        with db_relations_json.open("r", encoding="utf-8") as fh:
            for rec in json.load(fh):
                yield rec


def preprocess_ikraph_edges(db_relations_json: Path, output_path: Path) -> int:
    """
    Read DBRelations.json, remove self-loops, deduplicate on
    (node_one_id, node_two_id, relationship_type), and write a CSV.

    The direction field is preserved as-is; the edge-building step applies
    the swap when direction == "21".

    Returns the number of unique edges written.
    """
    print(f"[preprocess] streaming {db_relations_json.name}...", file=sys.stderr)

    dedupe_db = output_path.with_suffix(output_path.suffix + ".dedupe.sqlite")
    if dedupe_db.exists():
        dedupe_db.unlink()
    conn = _init_sqlite(dedupe_db)
    conn.execute(
        """CREATE TABLE u (
          n1 TEXT, n2 TEXT, rel TEXT,
          direction TEXT, source TEXT, correlation_type TEXT,
          n1t TEXT, n2t TEXT, n1n TEXT, n2n TEXT,
          prob TEXT, method TEXT, score TEXT, relID TEXT,
          PRIMARY KEY (n1, n2, rel)
        )"""
    )

    processed = skipped_self = 0
    batch: list[tuple] = []
    ins = (
        "INSERT OR IGNORE INTO u VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    )
    flush_at = 100_000

    for rec in _iter_db_relations_records(db_relations_json):
        n1 = str(rec.get("node_one_id") or "").strip()
        n2 = str(rec.get("node_two_id") or "").strip()
        rel = str(rec.get("relationship_type") or "").strip()
        if not n1 or not n2 or not rel:
            continue
        if n1 == n2:
            skipped_self += 1
            continue
        batch.append(
            (
                n1,
                n2,
                rel,
                str(rec.get("direction") or "0"),
                str(rec.get("source") or ""),
                str(rec.get("correlation_type") or ""),
                str(rec.get("node_one_type") or ""),
                str(rec.get("node_two_type") or ""),
                str(rec.get("node_one_name") or ""),
                str(rec.get("node_two_name") or ""),
                str(rec.get("prob") or ""),
                str(rec.get("method") or ""),
                str(rec.get("score") or ""),
                str(rec.get("relID") or ""),
            )
        )
        processed += 1
        if len(batch) >= flush_at:
            conn.executemany(ins, batch)
            conn.commit()
            batch.clear()
            print(f"[preprocess] ikraph streamed {processed:,} records...", file=sys.stderr)

    if batch:
        conn.executemany(ins, batch)
        conn.commit()

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
        cur = conn.execute("SELECT * FROM u ORDER BY n1, n2, rel")
        written = 0
        for row in cur:
            writer.writerow(row)
            written += 1

    conn.close()
    dedupe_db.unlink(missing_ok=True)

    duplicates_removed = max(0, processed - written - skipped_self)
    print(
        f"[preprocess] iKraph: {written:,} unique edges written "
        f"({processed:,} scanned, {skipped_self:,} self-loops, "
        f"~{duplicates_removed:,} duplicate triples suppressed)",
        file=sys.stderr,
    )
    return written


def preprocess_semmed_edges(connections_csv: Path, output_path: Path) -> int:
    """
    Read SemMedDB connections.csv (headerless: col0=source, col1=target, col2=predicate),
    remove self-loops, deduplicate on (source, target, predicate).

    Uses SQLite instead of a Python set so memory stays bounded on multi‑billion‑row files.

    Returns the number of unique edges written.
    """
    print(f"[preprocess] streaming {connections_csv.name} (SQLite dedupe)...", file=sys.stderr)

    dedupe_db = output_path.with_suffix(output_path.suffix + ".dedupe.sqlite")
    if dedupe_db.exists():
        dedupe_db.unlink()
    conn = _init_sqlite(dedupe_db)
    conn.execute(
        "CREATE TABLE u (src TEXT, tgt TEXT, pred TEXT, rest TEXT, PRIMARY KEY (src, tgt, pred))"
    )
    ins = "INSERT OR IGNORE INTO u VALUES (?,?,?,?)"

    processed = skipped_self = 0
    batch: list[tuple[str, str, str, str]] = []
    flush_at = 500_000

    with connections_csv.open("r", encoding="utf-8", errors="replace") as infile:
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
            row = parts + [""] * (4 - len(parts))
            rest = row[3] if len(row) > 3 else ""
            batch.append((src, tgt, pred, rest))
            processed += 1
            if len(batch) >= flush_at:
                conn.executemany(ins, batch)
                conn.commit()
                batch.clear()
                print(f"[preprocess] semmed connections scanned {processed:,} lines...", file=sys.stderr)

    if batch:
        conn.executemany(ins, batch)
        conn.commit()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)
        writer.writerow([":START_ID", ":END_ID", ":TYPE", "frequency%"])
        cur = conn.execute("SELECT src, tgt, pred, rest FROM u ORDER BY src, tgt, pred")
        rows_written = 0
        for src, tgt, pred, rest in cur:
            writer.writerow([src, tgt, pred, rest])
            rows_written += 1

    conn.close()
    dedupe_db.unlink(missing_ok=True)

    duplicates_removed = processed - rows_written - skipped_self
    print(
        f"[preprocess] SemMed: {rows_written:,} unique edges written "
        f"({processed:,} lines scanned, {skipped_self:,} self-loops, "
        f"~{duplicates_removed:,} duplicates suppressed)",
        file=sys.stderr,
    )
    return rows_written
