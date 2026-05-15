"""
Step 3 — build_normalized_import_csvs

Aggregates SemMedDB and iKraph edges into four Neo4j-import-ready CSVs:
  semmed_nodes_normalized.csv
  semmed_edges_normalized.csv
  ikraph_nodes_normalized.csv
  ikraph_edges_normalized.csv

Key differences from the original build_semmed_ikraph_normalized_import.py:
  1. Normalization input is always the JSON (no pre-computed semmed_nodes CSV needed).
  2. iKraph edge direction is respected: "21" → swap subject/object in both edge and PMID
     inserts so the join key is always consistent.
  3. provided_by:string[] column on every edge CSV row, sourced from the DBRelations.json
     "source" field (mapped to infores CURIEs) or "infores:ikraph" / "infores:semmeddb".
  4. Biolink predicates are validated against a known set at startup; unrecognised values
     emit warnings but do not block the run.
  5. subject_is_node_one from the reltype map can trigger an additional swap so the
     canonical Biolink subject domain is honoured after the direction swap.
"""
from __future__ import annotations

import csv
import gzip
import json
import re
import sqlite3
import sys
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Biolink predicate validation
# ---------------------------------------------------------------------------

BIOLINK_RE = re.compile(r"^biolink:[a-z][a-z0-9_]*$")

# Canonical predicates used in this pipeline; extend as needed.
KNOWN_BIOLINK_PREDICATES: frozenset[str] = frozenset(
    {
        "biolink:related_to",
        "biolink:subclass_of",
        "biolink:part_of",
        "biolink:participates_in",
        "biolink:treats",
        "biolink:prevents",
        "biolink:predisposes",
        "biolink:causes",
        "biolink:affects",
        "biolink:positively_regulates",
        "biolink:negatively_regulates",
        "biolink:interacts_with",
        "biolink:disrupts",
        "biolink:has_phenotype",
        "biolink:located_in",
        "biolink:produces",
        "biolink:uses",
        "biolink:associated_with",
        "biolink:gene_associated_with_condition",
        "biolink:expressed_in",
        "biolink:has_part",
        "biolink:related_to_at_concept_level",
        "biolink:coexists_with",
        "biolink:contributes_to",
        "biolink:regulates",
        "biolink:binds",
        "biolink:physically_interacts_with",
        "biolink:decreases_activity_of",
        "biolink:increases_activity_of",
        "biolink:decreases_expression_of",
        "biolink:increases_expression_of",
        "biolink:decreases_molecular_interaction",
        "biolink:increases_molecular_interaction",
    }
)


def validate_biolink_predicate(pred: str, context: str = "") -> bool:
    """
    Return True if pred is a recognised canonical Biolink predicate.
    Emit a warning (not an error) for unrecognised values.
    """
    if not BIOLINK_RE.match(pred):
        warnings.warn(
            f"Non-canonical Biolink predicate {pred!r}{' (' + context + ')' if context else ''}",
            stacklevel=2,
        )
        return False
    if pred not in KNOWN_BIOLINK_PREDICATES:
        warnings.warn(
            f"Unknown Biolink predicate {pred!r}{' (' + context + ')' if context else ''}."
            " Add it to KNOWN_BIOLINK_PREDICATES if intentional.",
            stacklevel=2,
        )
        return False
    return True


# ---------------------------------------------------------------------------
# SemMed predicate map (SemMed uppercase string → Biolink predicate)
# ---------------------------------------------------------------------------

SEMMED_PREDICATE_MAP: Dict[str, str] = {
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
    "ASSOCIATED_WITH": "biolink:related_to",       # intentionally broad
    "COEXISTS_WITH": "biolink:related_to",         # intentionally broad
    "CONVERTS_TO": "biolink:related_to",           # intentionally broad
    "DISRUPTS": "biolink:disrupts",
    "MANIFESTATION_OF": "biolink:has_phenotype",
    "LOCATION_OF": "biolink:located_in",
    "PRODUCES": "biolink:produces",
    "USES": "biolink:uses",
}

# ---------------------------------------------------------------------------
# iKraph source → infores CURIE
# ---------------------------------------------------------------------------

IKRAPH_SOURCE_TO_INFORES: Dict[str, str] = {
    "PubChem": "infores:pubchem",
    "primeKG": "infores:primekg",
    "Hetionet": "infores:hetionet",
    "GO_annotation": "infores:go",
    "TTD": "infores:ttd",
}
IKRAPH_PUBMED_INFORES = "infores:ikraph"
SEMMED_INFORES = "infores:semmeddb"

# Direction constants from iKraph metadata
DIR_FORWARD = "12"     # node_one → node_two (keep)
DIR_REVERSE = "21"     # node_two → node_one (swap)
DIR_UNDIRECTED = "0"   # no direction (keep)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_pmid(value: str) -> Optional[str]:
    v = value.strip()
    if not v:
        return None
    lower = v.lower()
    if lower.startswith("pmid:"):
        v = v.split(":", 1)[1].strip()
    if v.startswith("P") and v[1:].isdigit():
        v = v[1:]
    if v.isdigit():
        return f"PMID:{v}"
    return None


def _init_sqlite(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


# ---------------------------------------------------------------------------
# Normalization JSON loading
# ---------------------------------------------------------------------------


def _load_norm_json(path: Path) -> Dict[str, dict]:
    """Load a NodeNorm-format JSON (keyed by external_id). Uses ijson when available."""
    try:
        import ijson  # type: ignore[import-untyped]
    except ImportError:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    out: Dict[str, dict] = {}
    with path.open("rb") as fh:
        for key, value in ijson.kvitems(fh, ""):
            if isinstance(value, dict):
                out[str(key)] = value
    return out


def _materialize_semmed_nodes(
    norm_json: Dict[str, dict],
) -> Tuple[Dict[str, str], Dict[str, Dict]]:
    """
    Derive the SemMed CUI → normalized_id mapping and node metadata from the
    normalization JSON, replacing the legacy semmed_nodes_normalized.csv input.

    Returns (semmed_id_to_norm_id, norm_nodes_dict).
    """
    semmed_to_norm: Dict[str, str] = {}
    nodes: Dict[str, Dict] = {}
    for semmed_id, entry in norm_json.items():
        if not entry:
            continue
        id_block = entry.get("id")
        if not isinstance(id_block, dict):
            continue
        norm_id = (id_block.get("identifier") or "").strip()
        if not norm_id:
            continue
        semmed_to_norm[semmed_id] = norm_id
        if norm_id not in nodes:
            label = (id_block.get("label") or "").strip()
            types_raw = entry.get("type") or []
            nodes[norm_id] = {
                "name": label,
                "types": set(str(t) for t in types_raw) if isinstance(types_raw, list) else set(),
            }
        else:
            # Merge types from additional aliases
            types_raw = entry.get("type") or []
            if isinstance(types_raw, list):
                nodes[norm_id]["types"].update(str(t) for t in types_raw)
    return semmed_to_norm, nodes


def _materialize_ikraph_nodes(
    ner_json: Path,
    norm_json: Dict[str, dict],
    node_map_db: Path,
) -> int:
    """
    Build ikraph_nodes_normalized.csv and populate a biokdeid→norm_id SQLite map.
    Returns the count of unique normalized nodes written.
    """
    with ner_json.open("r", encoding="utf-8") as fh:
        entities = json.load(fh)

    conn = _init_sqlite(node_map_db)
    conn.execute("CREATE TABLE node_map (biokdeid TEXT PRIMARY KEY, norm_id TEXT)")
    conn.commit()

    nodes_out = node_map_db.parent / "ikraph_nodes_normalized.csv"
    seen_norm_ids: set = set()
    batch: List[Tuple[str, str]] = []
    processed = 0

    with nodes_out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["normalized_id:ID", "id", "name", "categories:string[]", "category:string[]", ":LABEL"]
        )
        for entity in entities:
            biokdeid = str(entity.get("biokdeid") or "").strip()
            ext_id = str(entity.get("id") or "").strip()
            if not biokdeid or not ext_id or ext_id == "NA":
                continue
            entry = norm_json.get(ext_id)
            if not entry:
                continue
            id_block = entry.get("id")
            if not isinstance(id_block, dict):
                continue
            norm_id = (id_block.get("identifier") or "").strip()
            if not norm_id:
                continue

            official = str(entity.get("official name") or entity.get("official_name") or "").strip()
            common = str(entity.get("common name") or entity.get("common_name") or "").strip()
            label = (id_block.get("label") or common or official or "").strip()
            types_raw = entry.get("type") or []
            types_str = ";".join(str(t) for t in types_raw) if isinstance(types_raw, list) else ""

            batch.append((biokdeid, norm_id))
            if len(batch) >= 50_000:
                conn.executemany("INSERT OR IGNORE INTO node_map VALUES (?, ?)", batch)
                conn.commit()
                batch.clear()

            if norm_id not in seen_norm_ids:
                writer.writerow([norm_id, norm_id, label, types_str, types_str, "IKraphEntity"])
                seen_norm_ids.add(norm_id)

            processed += 1
            if processed % 50_000 == 0:
                print(f"[ikraph] {processed:,} entities processed", file=sys.stderr)

    if batch:
        conn.executemany("INSERT OR IGNORE INTO node_map VALUES (?, ?)", batch)
        conn.commit()
    conn.close()
    print(
        f"[ikraph] {processed:,} entities → {len(seen_norm_ids):,} unique normalized nodes",
        file=sys.stderr,
    )
    return len(seen_norm_ids)


# ---------------------------------------------------------------------------
# SemMed edge building
# ---------------------------------------------------------------------------


def build_semmed_edges(
    predication_path: Path,
    semmed_to_norm: Dict[str, str],
    output_dir: Path,
) -> Path:
    """
    Aggregate SemMedDB predications into semmed_edges_normalized.csv.

    Deduplicates on (subj_norm, obj_norm, predicate), counts frequency, collects PMIDs.
    Adds provided_by = infores:semmeddb on every row.
    """
    db_path = output_dir / "semmed_edges_norm.sqlite"
    if db_path.exists():
        db_path.unlink()
    conn = _init_sqlite(db_path)
    conn.execute("PRAGMA cache_size=-200000")
    conn.execute("PRAGMA mmap_size=536870912")
    conn.execute("CREATE TABLE edges_stg (subj TEXT, obj TEXT, pred TEXT, biolink_pred TEXT)")
    conn.execute("CREATE TABLE pmids_stg (subj TEXT, obj TEXT, pred TEXT, pmid TEXT)")
    conn.commit()

    ins_edge = "INSERT INTO edges_stg VALUES (?,?,?,?)"
    ins_pmid = "INSERT INTO pmids_stg VALUES (?,?,?,?)"
    batch_edges: List[Tuple[str, str, str, str]] = []
    batch_pmids: List[Tuple[str, str, str, str]] = []
    processed = 0
    flush_n = 500_000

    open_fn = gzip.open if str(predication_path).endswith(".gz") else open
    with open_fn(predication_path, "rt", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if not row or row[0].strip().upper().startswith("PREDICATION_ID"):
                continue
            if len(row) < 6:
                continue
            pmid_raw = row[2].strip()
            pred = row[3].strip()
            subj = row[4].strip()
            obj = row[5].strip()
            if not subj or not obj or subj == obj:
                continue
            subj_n = semmed_to_norm.get(subj)
            obj_n = semmed_to_norm.get(obj)
            if not subj_n or not obj_n:
                continue
            biolink_pred = SEMMED_PREDICATE_MAP.get(pred.upper(), "biolink:related_to")
            batch_edges.append((subj_n, obj_n, pred, biolink_pred))
            pmid = _normalize_pmid(pmid_raw)
            if pmid:
                batch_pmids.append((subj_n, obj_n, pred, pmid))
            processed += 1
            if processed % flush_n == 0:
                conn.executemany(ins_edge, batch_edges)
                conn.executemany(ins_pmid, batch_pmids)
                conn.commit()
                batch_edges.clear()
                batch_pmids.clear()
                print(f"[semmed] {processed:,} rows processed", file=sys.stderr)

    if batch_edges:
        conn.executemany(ins_edge, batch_edges)
        conn.executemany(ins_pmid, batch_pmids)
        conn.commit()

    print("[semmed] aggregating...", file=sys.stderr)
    conn.execute(
        "CREATE TABLE edges AS "
        "SELECT subj, obj, pred, biolink_pred, COUNT(*) AS freq "
        "FROM edges_stg GROUP BY subj, obj, pred, biolink_pred"
    )
    conn.execute("CREATE TABLE pmids AS SELECT DISTINCT subj, obj, pred, pmid FROM pmids_stg")
    conn.execute("DROP TABLE edges_stg")
    conn.execute("DROP TABLE pmids_stg")
    conn.execute("CREATE INDEX pmids_join ON pmids (subj, obj, pred)")
    conn.commit()

    output_path = output_dir / "semmed_edges_normalized.csv"
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                ":START_ID", ":END_ID", ":TYPE",
                "biolink_predicate", "semmed_predicate",
                "frequency:int", "pmids:string[]",
                "provided_by:string[]",
                "subject", "object", "predicate",
                "knowledge_level", "agent_type",
            ]
        )
        cur = conn.cursor()
        for subj, obj, pred, biolink_pred, freq, pmids in cur.execute(
            "SELECT e.subj, e.obj, e.pred, e.biolink_pred, e.freq, "
            "IFNULL(GROUP_CONCAT(p.pmid, ';' ORDER BY p.pmid), '') AS pmids "
            "FROM edges e LEFT JOIN pmids p "
            "ON e.subj=p.subj AND e.obj=p.obj AND e.pred=p.pred "
            "GROUP BY e.subj, e.obj, e.pred, e.biolink_pred, e.freq"
        ):
            writer.writerow(
                [
                    subj, obj, "SEMMED_EDGE",
                    biolink_pred, pred, freq, pmids,
                    SEMMED_INFORES,
                    subj, obj, biolink_pred,
                    "statistical_association", "text_mining_agent",
                ]
            )

    conn.close()
    print(f"[semmed] wrote edges → {output_path}", file=sys.stderr)
    return output_path


# ---------------------------------------------------------------------------
# iKraph edge building (direction-aware, PMID-linked, provided_by)
# ---------------------------------------------------------------------------


def build_ikraph_edges(
    db_json: Path,
    pubmed_json: Path,
    node_map_db: Path,
    reltype_map: Dict[str, Tuple[str, bool]],
    output_dir: Path,
) -> Path:
    """
    Build ikraph_edges_normalized.csv from DBRelations.json and PubMedList.json.

    Direction handling:
      DBRelations.json "direction" field:
        "12" or "" → keep node_one as subject
        "21"       → swap node_one/node_two so node_two becomes subject
        "0"        → undirected; keep node_one as subject
      PubMedList.json id field parts[4]:
        "12" → keep n1 as subject
        "21" → swap so n2 becomes subject

    After the direction swap, if subject_is_node_one==False in the reltype map,
    an additional swap is applied so the canonical Biolink subject domain is honoured.

    PMID inserts always use the same (subj, obj) key as the corresponding edge insert,
    ensuring the final GROUP BY join is correct regardless of direction.

    provided_by:
      DB edges  → IKRAPH_SOURCE_TO_INFORES[source] or infores:ikraph
      PubMed edges → infores:ikraph
    """
    edge_db = output_dir / "ikraph_edges_norm.sqlite"
    if edge_db.exists():
        edge_db.unlink()
    conn = _init_sqlite(edge_db)
    conn.execute(
        "CREATE TABLE edges ("
        "subj TEXT, obj TEXT, rel_type TEXT, biolink_pred TEXT, provided_by TEXT, freq INTEGER,"
        " PRIMARY KEY (subj, obj, rel_type))"
    )
    conn.execute(
        "CREATE TABLE pmids (subj TEXT, obj TEXT, rel_type TEXT, pmid TEXT,"
        " PRIMARY KEY (subj, obj, rel_type, pmid))"
    )
    conn.commit()

    node_conn = sqlite3.connect(node_map_db)
    biokde_to_norm = dict(
        node_conn.execute("SELECT biokdeid, norm_id FROM node_map").fetchall()
    )
    node_conn.close()

    edge_upsert = (
        "INSERT INTO edges (subj, obj, rel_type, biolink_pred, provided_by, freq) "
        "VALUES (?,?,?,?,?,1) "
        "ON CONFLICT(subj, obj, rel_type) DO UPDATE SET freq=freq+1"
    )
    pmid_insert = "INSERT OR IGNORE INTO pmids (subj, obj, rel_type, pmid) VALUES (?,?,?,?)"

    batch_edges: List[Tuple[str, str, str, str, str]] = []
    batch_pmids: List[Tuple[str, str, str, str]] = []

    def _flush(conn: sqlite3.Connection) -> None:
        if batch_edges:
            conn.executemany(edge_upsert, batch_edges)
            batch_edges.clear()
        if batch_pmids:
            conn.executemany(pmid_insert, batch_pmids)
            batch_pmids.clear()
        conn.commit()

    # ── DB relationships ────────────────────────────────────────────────────
    print(f"[ikraph] loading DB relationships from {db_json.name}...", file=sys.stderr)
    with db_json.open("r", encoding="utf-8") as fh:
        db_rels = json.load(fh)

    processed = 0
    for rel in db_rels:
        n1 = str(rel.get("node_one_id") or "").strip()
        n2 = str(rel.get("node_two_id") or "").strip()
        if not n1 or not n2 or n1 == n2:
            continue
        rel_type = str(rel.get("relationship_type") or "").strip()
        if not rel_type:
            continue
        n1_norm = biokde_to_norm.get(n1)
        n2_norm = biokde_to_norm.get(n2)
        if not n1_norm or not n2_norm:
            continue

        biolink_pred, subject_is_node_one = reltype_map.get(rel_type, ("biolink:related_to", True))

        # Step 1: apply iKraph direction field
        direction = str(rel.get("direction") or DIR_UNDIRECTED).strip()
        subj_norm, obj_norm = n1_norm, n2_norm
        if direction == DIR_REVERSE:
            subj_norm, obj_norm = n2_norm, n1_norm

        # Step 2: apply Biolink canonical direction hint
        if not subject_is_node_one:
            subj_norm, obj_norm = obj_norm, subj_norm

        source = str(rel.get("source") or "").strip()
        provided_by = IKRAPH_SOURCE_TO_INFORES.get(source, IKRAPH_PUBMED_INFORES)

        batch_edges.append((subj_norm, obj_norm, rel_type, biolink_pred, provided_by))
        processed += 1
        if processed % 200_000 == 0:
            _flush(conn)
            print(f"[ikraph] {processed:,} DB relationships processed", file=sys.stderr)

    _flush(conn)
    print(f"[ikraph] {processed:,} DB relationships total", file=sys.stderr)

    # ── PubMed relationships ────────────────────────────────────────────────
    print(f"[ikraph] loading PubMed relationships from {pubmed_json.name}...", file=sys.stderr)
    with pubmed_json.open("r", encoding="utf-8") as fh:
        pubmed_rels = json.load(fh)

    processed = 0
    for entry in pubmed_rels:
        rel_id = str(entry.get("id") or "").strip()
        parts = rel_id.split(".")
        # id format: n1.n2.reltype.corrType.direction.method  (6 parts)
        if len(parts) < 3:
            continue
        n1, n2, rel_type = parts[0], parts[1], parts[2]
        if not n1 or not n2 or n1 == n2 or not rel_type:
            continue
        n1_norm = biokde_to_norm.get(n1)
        n2_norm = biokde_to_norm.get(n2)
        if not n1_norm or not n2_norm:
            continue

        biolink_pred, subject_is_node_one = reltype_map.get(rel_type, ("biolink:related_to", True))

        # Step 1: apply direction from id string parts[4]
        direction = parts[4] if len(parts) > 4 else DIR_FORWARD
        subj_norm, obj_norm = n1_norm, n2_norm
        if direction == DIR_REVERSE:
            subj_norm, obj_norm = n2_norm, n1_norm

        # Step 2: apply Biolink canonical direction hint
        if not subject_is_node_one:
            subj_norm, obj_norm = obj_norm, subj_norm

        batch_edges.append((subj_norm, obj_norm, rel_type, biolink_pred, IKRAPH_PUBMED_INFORES))

        # PMIDs: item[1] = "pmid.n2.n1"; PMID is the first component.
        # Use the same post-swap (subj_norm, obj_norm) key so the join is always consistent.
        for item in entry.get("list") or []:
            if not item or len(item) < 2:
                continue
            pmid_raw = str(item[1]).split(".")[0]
            pmid = _normalize_pmid(pmid_raw)
            if pmid:
                batch_pmids.append((subj_norm, obj_norm, rel_type, pmid))

        processed += 1
        if processed % 200_000 == 0:
            _flush(conn)
            print(f"[ikraph] {processed:,} PubMed relationships processed", file=sys.stderr)

    _flush(conn)
    print(f"[ikraph] {processed:,} PubMed relationships total", file=sys.stderr)

    # ── Export ──────────────────────────────────────────────────────────────
    output_path = output_dir / "ikraph_edges_normalized.csv"
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                ":START_ID", ":END_ID", ":TYPE",
                "biolink_predicate", "ikraph_relation_type",
                "frequency:int", "pmids:string[]",
                "provided_by:string[]",
                "subject", "object", "predicate",
                "knowledge_level", "agent_type",
            ]
        )
        cur = conn.cursor()
        for subj, obj, rel_type, biolink_pred, provided_by, freq, pmids in cur.execute(
            "SELECT e.subj, e.obj, e.rel_type, e.biolink_pred, e.provided_by, e.freq, "
            "IFNULL(GROUP_CONCAT(p.pmid, ';' ORDER BY p.pmid), '') AS pmids "
            "FROM edges e LEFT JOIN pmids p "
            "ON e.subj=p.subj AND e.obj=p.obj AND e.rel_type=p.rel_type "
            "GROUP BY e.subj, e.obj, e.rel_type, e.biolink_pred, e.provided_by, e.freq"
        ):
            writer.writerow(
                [
                    subj, obj, "IKRAPH_EDGE",
                    biolink_pred, rel_type, freq, pmids,
                    provided_by,
                    subj, obj, biolink_pred,
                    "prediction", "data_analysis_pipeline",
                ]
            )

    conn.close()
    print(f"[ikraph] wrote edges → {output_path}", file=sys.stderr)
    return output_path


# ---------------------------------------------------------------------------
# Write nodes CSV
# ---------------------------------------------------------------------------


def _write_nodes_csv(path: Path, nodes: Dict[str, Dict], label: str) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["normalized_id:ID", "id", "name", "categories:string[]", "category:string[]", ":LABEL"]
        )
        for norm_id, data in nodes.items():
            name = data.get("name") or ""
            types = sorted(data.get("types") or [])
            types_str = ";".join(types)
            writer.writerow([norm_id, norm_id, name, types_str, types_str, label])


# ---------------------------------------------------------------------------
# Main entry point for this step
# ---------------------------------------------------------------------------


def build_normalized_import_csvs(
    semmed_normalized_json: Path,
    ikraph_normalized_json: Path,
    predication_path: Path,
    ner_json: Path,
    db_json: Path,
    pubmed_json: Path,
    reltype_map: Dict[str, Tuple[str, bool]],
    output_dir: Path,
    *,
    stream_ikraph_json: bool = False,
) -> List[Path]:
    """
    Build all four normalized CSVs from the two normalization JSONs and the raw edge files.

    Args:
        semmed_normalized_json: NodeNorm output for SemMed (keyed by CUI)
        ikraph_normalized_json: NodeNorm output for iKraph (keyed by external_id)
        predication_path:       predication.csv or predication.csv.gz
        ner_json:               NER_ID_dict_cap_final.json (iKraph entities)
        db_json:                DBRelations.json (iKraph DB-sourced edges)
        pubmed_json:            PubMedList.json (iKraph PubMed-derived edges)
        reltype_map:            int_rep → (biolink_predicate, subject_is_node_one)
        output_dir:             directory for the four output CSVs
        stream_ikraph_json:     reserved for future ijson streaming; currently unused

    Returns list of the four output CSV paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Validate all Biolink predicates used in the SemMed map at startup
    for semmed_pred, biolink_pred in SEMMED_PREDICATE_MAP.items():
        validate_biolink_predicate(biolink_pred, context=f"SemMed:{semmed_pred}")

    # Validate predicates in the reltype map
    unknown_count = 0
    for int_rep, (pred, _) in reltype_map.items():
        if not validate_biolink_predicate(pred, context=f"iKraph int_rep={int_rep}"):
            unknown_count += 1
    if unknown_count:
        print(
            f"[validate] {unknown_count} iKraph predicates not in KNOWN_BIOLINK_PREDICATES"
            " (warnings emitted above)",
            file=sys.stderr,
        )

    # ── SemMed ──────────────────────────────────────────────────────────────
    print("[semmed] loading normalization JSON...", file=sys.stderr)
    semmed_norm_map = _load_norm_json(semmed_normalized_json)
    print(f"[semmed] {len(semmed_norm_map):,} normalization entries", file=sys.stderr)

    semmed_to_norm, semmed_nodes = _materialize_semmed_nodes(semmed_norm_map)
    print(
        f"[semmed] {len(semmed_to_norm):,} CUI mappings → {len(semmed_nodes):,} unique nodes",
        file=sys.stderr,
    )

    semmed_nodes_path = output_dir / "semmed_nodes_normalized.csv"
    _write_nodes_csv(semmed_nodes_path, semmed_nodes, "SemmedEntity")
    print(f"[semmed] wrote nodes → {semmed_nodes_path}", file=sys.stderr)

    semmed_edges_path = build_semmed_edges(predication_path, semmed_to_norm, output_dir)

    # ── iKraph ──────────────────────────────────────────────────────────────
    print("[ikraph] loading normalization JSON...", file=sys.stderr)
    ikraph_norm_map = _load_norm_json(ikraph_normalized_json)
    print(f"[ikraph] {len(ikraph_norm_map):,} normalization entries", file=sys.stderr)

    node_map_db = output_dir / "ikraph_node_map.sqlite"
    if node_map_db.exists():
        node_map_db.unlink()
    _materialize_ikraph_nodes(ner_json, ikraph_norm_map, node_map_db)

    ikraph_nodes_path = output_dir / "ikraph_nodes_normalized.csv"
    ikraph_edges_path = build_ikraph_edges(db_json, pubmed_json, node_map_db, reltype_map, output_dir)

    outputs = [semmed_nodes_path, semmed_edges_path, ikraph_nodes_path, ikraph_edges_path]
    print("[build_import_csvs] done.", file=sys.stderr)
    return outputs
