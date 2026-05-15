#!/usr/bin/env python3
"""
Build normalized Neo4j import files for SemMedDB and iKraph.

Outputs (in --output-dir):
  - semmed_nodes_normalized.csv
  - semmed_edges_normalized.csv
  - ikraph_nodes_normalized.csv
  - ikraph_edges_normalized.csv

SemMed edges are aggregated from predication.csv.gz with frequency counts and PMID lists.
iKraph edges are aggregated from relationships_db.mapped.csv.gz and relationships_pubmed.mapped.csv.gz.

SemMed edge ingest uses append-only SQLite staging tables, then a single GROUP BY / DISTINCT pass (faster
than per-row UPSERTs on large predication files; uses more temporary disk under --output-dir).

Streaming: predication/relationship inputs are already read row-by-row from gzip. Edge aggregation
requires keyed state (SQLite in this script, or an external sort+merge). The iKraph normalization
JSON can be parsed incrementally with ijson when installed; use --stream-ikraph-norm-json to write
a compact SQLite lookup table and avoid holding the full JSON object in RAM.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SEM_MED_PREDICATE_MAP = {
    "ISA": "biolink:subclass_of",
    "PART_OF": "biolink:part_of",
    "PROCESS_OF": "biolink:participates_in",
    "TREATS": "biolink:treats",
    "PREVENTS": "biolink:prevents",
    "PREDISPOSES": "biolink:predisposes",
    "CAUSES": "biolink:causes",
    "AFFECTS": "biolink:affects",
    "STIMULATES": "biolink:positively_regulates",
    "INHIBITS": "biolink:negatively_regulates",
    "INTERACTS_WITH": "biolink:interacts_with",
    "ASSOCIATED_WITH": "biolink:related_to",
    "COEXISTS_WITH": "biolink:related_to",
    "CONVERTS_TO": "biolink:related_to",
    "DISRUPTS": "biolink:disrupts",
    "MANIFESTATION_OF": "biolink:has_phenotype",
    "LOCATION_OF": "biolink:located_in",
    "PRODUCES": "biolink:produces",
    "USES": "biolink:uses",
}


def normalize_pmid(value: str) -> Optional[str]:
    if not value:
        return None
    pmid_val = value.strip()
    if not pmid_val:
        return None
    lower = pmid_val.lower()
    if lower.startswith("pmid:"):
        pmid_val = pmid_val.split(":", 1)[1].strip()
    if pmid_val.startswith("P") and pmid_val[1:].isdigit():
        pmid_val = pmid_val[1:]
    if pmid_val.isdigit():
        return f"PMID:{pmid_val}"
    return None


def load_ikraph_normalized_map(path: Path) -> Dict[str, dict]:
    """
    Load the root object mapping external_id -> normalization record.

    When ``ijson`` is installed, parses incrementally (lower peak memory than json.load).
    """
    try:
        import ijson  # type: ignore[import-untyped]
    except ImportError:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    out: Dict[str, dict] = {}
    with path.open("rb") as handle:
        for key, value in ijson.kvitems(handle, ""):
            if isinstance(value, dict):
                out[str(key)] = value
    return out


def materialize_ikraph_norm_sqlite(json_path: Path, sqlite_path: Path) -> None:
    """
    Stream the normalization JSON into a narrow SQLite table (requires ``ijson``).

    One row per external_id with columns needed for node CSV and biokdeid->norm_id mapping.
    """
    try:
        import ijson  # type: ignore[import-untyped]
    except ImportError as exc:
        raise SystemExit(
            "Use `pip install ijson` for --stream-ikraph-norm-json."
        ) from exc
    if sqlite_path.exists():
        sqlite_path.unlink()
    conn = init_sqlite(sqlite_path)
    conn.execute(
        "CREATE TABLE ikraph_norm (ext_id TEXT PRIMARY KEY, norm_id TEXT, label TEXT, types TEXT)"
    )
    conn.commit()
    insert = "INSERT INTO ikraph_norm (ext_id, norm_id, label, types) VALUES (?, ?, ?, ?)"
    batch: List[Tuple[str, str, str, str]] = []
    with json_path.open("rb") as handle:
        for key, value in ijson.kvitems(handle, ""):
            if not isinstance(value, dict):
                continue
            id_block = value.get("id")
            if not isinstance(id_block, dict):
                continue
            norm_id = id_block.get("identifier")
            if not norm_id:
                continue
            ext_id = str(key).strip()
            label = str(id_block.get("label") or "")
            types_raw = value.get("type") or []
            if isinstance(types_raw, list):
                types_str = ";".join(str(t) for t in types_raw)
            else:
                types_str = ""
            batch.append((ext_id, str(norm_id), label, types_str))
            if len(batch) >= 50_000:
                conn.executemany(insert, batch)
                batch.clear()
    if batch:
        conn.executemany(insert, batch)
    conn.commit()
    conn.close()


def load_ikraph_biolink_map(path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rel = (row.get("ikraph_relation_type") or "").strip()
            pred = (row.get("proposed_biolink_predicate") or "").strip()
            if rel and pred:
                mapping[rel] = pred
    return mapping


def load_semmed_nodes(semmed_nodes_path: Path) -> Tuple[Dict[str, str], Dict[str, Dict[str, object]]]:
    """
    Returns:
      - semmed_id -> normalized_id mapping
      - normalized node dict keyed by normalized_id with name + categories
    """
    semmed_to_norm: Dict[str, str] = {}
    nodes: Dict[str, Dict[str, object]] = {}
    with semmed_nodes_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            semmed_id = (row.get("semmed_id:ID") or row.get("semmed_id") or "").strip()
            norm_id = (row.get("normalized_id") or "").strip()
            if not semmed_id or not norm_id:
                continue
            semmed_to_norm[semmed_id] = norm_id
            label = (row.get("normalized_label") or row.get("name") or "").strip()
            types_field = (row.get("normalized_types:string[]") or row.get("normalized_types") or "").strip()
            types = [t for t in types_field.split(";") if t]
            if norm_id not in nodes:
                nodes[norm_id] = {
                    "name": label,
                    "types": set(types),
                }
            else:
                nodes[norm_id]["types"].update(types)
                if not nodes[norm_id]["name"] and label:
                    nodes[norm_id]["name"] = label
    return semmed_to_norm, nodes


def write_nodes_csv(path: Path, nodes: Dict[str, Dict[str, object]], label: str) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["normalized_id:ID", "id", "name", "categories:string[]", "category:string[]", ":LABEL"])
        for norm_id, data in nodes.items():
            name = data.get("name", "") or ""
            types = sorted(data.get("types", []))
            types_str = ";".join(types)
            writer.writerow([norm_id, norm_id, name, types_str, types_str, label])


def init_sqlite(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def build_semmed_edges(predication_path: Path, semmed_to_norm: Dict[str, str], output_dir: Path) -> Path:
    db_path = output_dir / "semmed_edges_norm.sqlite"
    if db_path.exists():
        db_path.unlink()
    conn = init_sqlite(db_path)
    conn.execute("PRAGMA cache_size = -200000")
    conn.execute("PRAGMA mmap_size = 536870912")
    conn.execute(
        "CREATE TABLE edges_stg (subj TEXT, obj TEXT, pred TEXT, biolink_pred TEXT)"
    )
    conn.execute("CREATE TABLE pmids_stg (subj TEXT, obj TEXT, pred TEXT, pmid TEXT)")
    conn.commit()

    ins_stg_edge = "INSERT INTO edges_stg VALUES (?,?,?,?)"
    ins_stg_pmid = "INSERT INTO pmids_stg VALUES (?,?,?,?)"

    batch_edges: List[Tuple[str, str, str, str]] = []
    batch_pmids: List[Tuple[str, str, str, str]] = []
    processed = 0
    stg_flush = 500_000

    with gzip.open(predication_path, "rt", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            if row[0].strip().upper().startswith("PREDICATION_ID"):
                continue
            if len(row) < 6:
                continue
            pmid = row[2].strip()
            pred = row[3].strip()
            subj = row[4].strip()
            obj = row[5].strip()
            if not subj or not obj or subj == obj:
                continue
            subj_norm = semmed_to_norm.get(subj)
            obj_norm = semmed_to_norm.get(obj)
            if not subj_norm or not obj_norm:
                continue
            biolink_pred = SEM_MED_PREDICATE_MAP.get(pred.upper(), "biolink:related_to")
            batch_edges.append((subj_norm, obj_norm, pred, biolink_pred))
            pmid_norm = normalize_pmid(pmid)
            if pmid_norm:
                batch_pmids.append((subj_norm, obj_norm, pred, pmid_norm))
            processed += 1
            if processed % stg_flush == 0:
                conn.executemany(ins_stg_edge, batch_edges)
                conn.executemany(ins_stg_pmid, batch_pmids)
                conn.commit()
                batch_edges.clear()
                batch_pmids.clear()
                print(f"[semmed] processed {processed:,} rows", file=sys.stderr)

    if batch_edges:
        conn.executemany(ins_stg_edge, batch_edges)
        conn.executemany(ins_stg_pmid, batch_pmids)
        conn.commit()

    print("[semmed] aggregating staging tables...", file=sys.stderr)
    conn.execute(
        "CREATE TABLE edges AS "
        "SELECT subj, obj, pred, biolink_pred, COUNT(*) AS freq "
        "FROM edges_stg GROUP BY subj, obj, pred, biolink_pred"
    )
    conn.execute(
        "CREATE TABLE pmids AS SELECT DISTINCT subj, obj, pred, pmid FROM pmids_stg"
    )
    conn.execute("DROP TABLE edges_stg")
    conn.execute("DROP TABLE pmids_stg")
    conn.execute("CREATE INDEX pmids_join ON pmids (subj, obj, pred)")
    conn.commit()

    output_path = output_dir / "semmed_edges_normalized.csv"
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            ":START_ID",
            ":END_ID",
            ":TYPE",
            "biolink_predicate",
            "semmed_predicate",
            "frequency:int",
            "pmids:string[]",
            "subject",
            "object",
            "predicate",
            "knowledge_level",
            "agent_type",
        ])
        cur = conn.cursor()
        # One grouped query avoids N+1 SELECTs per edge (dominant cost on large graphs).
        export_sql = (
            "SELECT e.subj, e.obj, e.pred, e.biolink_pred, e.freq, "
            "IFNULL(GROUP_CONCAT(p.pmid, ';' ORDER BY p.pmid), '') AS pmids "
            "FROM edges e LEFT JOIN pmids p "
            "ON e.subj = p.subj AND e.obj = p.obj AND e.pred = p.pred "
            "GROUP BY e.subj, e.obj, e.pred, e.biolink_pred, e.freq"
        )
        for subj, obj, pred, biolink_pred, freq, pmids in cur.execute(export_sql):
            writer.writerow([
                subj, obj, "SEMMED_EDGE", biolink_pred, pred, freq, pmids,
                subj, obj, biolink_pred, "statistical_association", "text_mining_agent",
            ])

    conn.close()
    return output_path


def build_ikraph_nodes_and_map(
    nodes_dir: Path,
    output_dir: Path,
    *,
    normalized_map: Optional[Dict[str, dict]] = None,
    norm_sqlite: Optional[Path] = None,
) -> Path:
    if (normalized_map is None) == (norm_sqlite is None):
        raise ValueError("Specify exactly one of normalized_map= or norm_sqlite=.")

    norm_conn: Optional[sqlite3.Connection] = None
    norm_sql = "SELECT norm_id, label, types FROM ikraph_norm WHERE ext_id = ?"
    if norm_sqlite is not None:
        norm_uri = norm_sqlite.resolve().as_uri() + "?mode=ro"
        norm_conn = sqlite3.connect(norm_uri, uri=True)

    node_map_db = output_dir / "ikraph_node_map.sqlite"
    if node_map_db.exists():
        node_map_db.unlink()
    conn = init_sqlite(node_map_db)
    conn.execute("CREATE TABLE node_map (biokdeid TEXT PRIMARY KEY, norm_id TEXT)")
    conn.commit()
    map_insert = "INSERT OR IGNORE INTO node_map (biokdeid, norm_id) VALUES (?, ?)"
    map_batch: List[Tuple[str, str]] = []
    _MAP_BATCH = 50_000

    nodes_out = output_dir / "ikraph_nodes_normalized.csv"
    seen_norm_ids = set()
    with nodes_out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["normalized_id:ID", "id", "name", "categories:string[]", "category:string[]", ":LABEL"])
        for nodes_path in sorted(nodes_dir.glob("nodes_*.csv.gz")):
            with gzip.open(nodes_path, "rt", encoding="utf-8", errors="replace", newline="") as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    biokdeid = (row.get("biokdeid:ID") or "").strip()
                    ext_id = (row.get("external_id") or "").strip()
                    if not biokdeid or not ext_id or ext_id == "NA":
                        continue
                    if normalized_map is not None:
                        norm_entry = normalized_map.get(ext_id)
                        if not norm_entry or not norm_entry.get("id"):
                            continue
                        norm_id = norm_entry["id"].get("identifier")
                        if not norm_id:
                            continue
                        label = (
                            norm_entry["id"].get("label")
                            or row.get("common_name")
                            or row.get("official_name")
                            or ""
                        )
                        types_raw = norm_entry.get("type") or []
                        types_joined = (
                            ";".join(str(t) for t in types_raw)
                            if isinstance(types_raw, list)
                            else ""
                        )
                    else:
                        assert norm_conn is not None
                        nrow = norm_conn.execute(norm_sql, (ext_id,)).fetchone()
                        if not nrow:
                            continue
                        norm_id, norm_label, types_str = nrow[0], nrow[1], nrow[2]
                        label = (
                            (norm_label or "")
                            or row.get("common_name")
                            or row.get("official_name")
                            or ""
                        )
                        types_joined = types_str or ""
                    map_batch.append((biokdeid, norm_id))
                    if len(map_batch) >= _MAP_BATCH:
                        conn.executemany(map_insert, map_batch)
                        map_batch.clear()
                    if norm_id in seen_norm_ids:
                        continue
                    writer.writerow([norm_id, norm_id, label, types_joined, types_joined, "IKraphEntity"])
                    seen_norm_ids.add(norm_id)
            if map_batch:
                conn.executemany(map_insert, map_batch)
                map_batch.clear()
            conn.commit()
            print(f"[ikraph] processed {nodes_path.name}", file=sys.stderr)

    conn.close()
    if norm_conn is not None:
        norm_conn.close()
    return node_map_db


def build_ikraph_edges(
    relationships_db: Path,
    relationships_pubmed: Path,
    node_map_db: Path,
    reltype_map: Dict[str, str],
    output_dir: Path,
) -> Path:
    edge_db = output_dir / "ikraph_edges_norm.sqlite"
    if edge_db.exists():
        edge_db.unlink()
    conn = init_sqlite(edge_db)
    conn.execute(
        "CREATE TABLE edges (subj TEXT, obj TEXT, rel_type TEXT, biolink_pred TEXT, freq INTEGER, "
        "PRIMARY KEY (subj, obj, rel_type))"
    )
    conn.execute(
        "CREATE TABLE pmids (subj TEXT, obj TEXT, rel_type TEXT, pmid TEXT, "
        "PRIMARY KEY (subj, obj, rel_type, pmid))"
    )
    conn.commit()

    node_conn = sqlite3.connect(node_map_db)
    biokde_to_norm = dict(node_conn.execute("SELECT biokdeid, norm_id FROM node_map").fetchall())
    node_conn.close()

    edge_upsert = (
        "INSERT INTO edges (subj, obj, rel_type, biolink_pred, freq) VALUES (?, ?, ?, ?, 1) "
        "ON CONFLICT(subj, obj, rel_type) DO UPDATE SET freq = freq + 1"
    )
    pmid_insert = "INSERT OR IGNORE INTO pmids (subj, obj, rel_type, pmid) VALUES (?, ?, ?, ?)"

    def process_relationships(path: Path, has_pmid: bool) -> None:
        processed = 0
        batch_edges: List[Tuple[str, str, str, str]] = []
        batch_pmids: List[Tuple[str, str, str, str]] = []
        with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                subj = (row.get(":START_ID") or "").strip()
                obj = (row.get(":END_ID") or "").strip()
                if not subj or not obj or subj == obj:
                    continue
                rel_type = (row.get("relationship_type") or row.get("relation_id") or "").strip()
                if not rel_type:
                    continue
                subj_norm = biokde_to_norm.get(subj)
                obj_norm = biokde_to_norm.get(obj)
                if not subj_norm or not obj_norm:
                    continue
                biolink_pred = reltype_map.get(rel_type, "biolink:related_to")
                batch_edges.append((subj_norm, obj_norm, rel_type, biolink_pred))
                if has_pmid:
                    pmid = normalize_pmid((row.get("document_id") or "").strip())
                    if pmid:
                        batch_pmids.append((subj_norm, obj_norm, rel_type, pmid))
                processed += 1
                if processed % 200000 == 0:
                    conn.executemany(edge_upsert, batch_edges)
                    conn.executemany(pmid_insert, batch_pmids)
                    conn.commit()
                    batch_edges.clear()
                    batch_pmids.clear()
                    print(f"[ikraph] processed {processed:,} rows from {path.name}", file=sys.stderr)
        if batch_edges:
            conn.executemany(edge_upsert, batch_edges)
            conn.executemany(pmid_insert, batch_pmids)
            conn.commit()

    process_relationships(relationships_db, has_pmid=False)
    process_relationships(relationships_pubmed, has_pmid=True)

    output_path = output_dir / "ikraph_edges_normalized.csv"
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            ":START_ID",
            ":END_ID",
            ":TYPE",
            "biolink_predicate",
            "ikraph_relation_type",
            "frequency:int",
            "pmids:string[]",
            "subject",
            "object",
            "predicate",
            "knowledge_level",
            "agent_type",
        ])
        cur = conn.cursor()
        export_sql = (
            "SELECT e.subj, e.obj, e.rel_type, e.biolink_pred, e.freq, "
            "IFNULL(GROUP_CONCAT(p.pmid, ';' ORDER BY p.pmid), '') AS pmids "
            "FROM edges e LEFT JOIN pmids p "
            "ON e.subj = p.subj AND e.obj = p.obj AND e.rel_type = p.rel_type "
            "GROUP BY e.subj, e.obj, e.rel_type, e.biolink_pred, e.freq"
        )
        for subj, obj, rel_type, biolink_pred, freq, pmids in cur.execute(export_sql):
            writer.writerow([
                subj, obj, "IKRAPH_EDGE", biolink_pred, rel_type, freq, pmids,
                subj, obj, biolink_pred, "prediction", "data_analysis_pipeline",
            ])

    conn.close()
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build normalized SemMedDB + iKraph import CSVs.")
    parser.add_argument("--semmed-nodes", type=Path, required=True)
    parser.add_argument("--semmed-predication", type=Path, required=True)
    parser.add_argument("--ikraph-nodes-dir", type=Path, required=True)
    parser.add_argument("--ikraph-relationships-db", type=Path, required=True)
    parser.add_argument("--ikraph-relationships-pubmed", type=Path, required=True)
    parser.add_argument("--ikraph-normalized-json", type=Path, required=True)
    parser.add_argument("--ikraph-reltype-map", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--stream-ikraph-norm-json",
        action="store_true",
        help="Stream iKraph JSON into ikraph_norm_lookup.sqlite via ijson; avoid dict of full JSON in RAM (slower node pass).",
    )

    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("[semmed] loading normalized nodes...", file=sys.stderr)
    semmed_to_norm, semmed_nodes = load_semmed_nodes(args.semmed_nodes)
    semmed_nodes_out = args.output_dir / "semmed_nodes_normalized.csv"
    write_nodes_csv(semmed_nodes_out, semmed_nodes, "SemmedEntity")
    print(f"[semmed] wrote nodes -> {semmed_nodes_out}", file=sys.stderr)

    print("[semmed] building normalized edges...", file=sys.stderr)
    build_semmed_edges(args.semmed_predication, semmed_to_norm, args.output_dir)

    print("[ikraph] loading relation mapping...", file=sys.stderr)
    reltype_map = load_ikraph_biolink_map(args.ikraph_reltype_map)

    print("[ikraph] building nodes + mapping...", file=sys.stderr)
    if args.stream_ikraph_norm_json:
        norm_lookup = args.output_dir / "ikraph_norm_lookup.sqlite"
        print(f"[ikraph] streaming normalization JSON -> {norm_lookup.name}", file=sys.stderr)
        materialize_ikraph_norm_sqlite(args.ikraph_normalized_json, norm_lookup)
        node_map_db = build_ikraph_nodes_and_map(
            args.ikraph_nodes_dir,
            args.output_dir,
            norm_sqlite=norm_lookup,
        )
    else:
        print("[ikraph] loading normalized json...", file=sys.stderr)
        ikraph_norm = load_ikraph_normalized_map(args.ikraph_normalized_json)
        node_map_db = build_ikraph_nodes_and_map(
            args.ikraph_nodes_dir,
            args.output_dir,
            normalized_map=ikraph_norm,
        )

    print("[ikraph] building normalized edges...", file=sys.stderr)
    build_ikraph_edges(
        args.ikraph_relationships_db,
        args.ikraph_relationships_pubmed,
        node_map_db,
        reltype_map,
        args.output_dir,
    )

    print("[done] normalized import files generated.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
