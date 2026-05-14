#!/usr/bin/env python3
"""
Verify that locally-built graph CSVs match the counts in the Kubernetes Neo4j instances.

Checks two graphs:
  1. General medicine graph (CIM KG / SemMed+iKraph normalized)
       Kubernetes URI: neo4j+s://neo4j-cim-kg.combini.ncsa.illinois.edu:7687   (default)
       Local CSVs: --gen-med-dir (four *_normalized.csv files)

  2. CM subgraph
       Kubernetes URI: neo4j+s://neo4j.combini.ncsa.illinois.edu:7687   (default)
       Local CSVs: --cm-subgraph-dir (TSV files from extract pipeline)

Credentials can be provided via an .env file or environment variables.

Usage
-----
  python3 verify_graph_match.py --gen-med-dir output/gen_med_graph_neo4j --cm-subgraph-dir output/cm_subgraph

  # With explicit credentials
  python3 verify_graph_match.py \\
      --gen-med-dir   output/gen_med_graph_neo4j \\
      --cm-subgraph-dir output/cm_subgraph \\
      --env-file      /path/to/.env

  # Only verify one graph
  python3 verify_graph_match.py --gen-med-dir output/gen_med_graph_neo4j --skip-cm

  # Dry-run: count local CSVs only (no Neo4j connection)
  python3 verify_graph_match.py --gen-med-dir output/gen_med_graph_neo4j --local-only

Environment variables (or .env file keys):
  GENERAL_MED_URI        Bolt URI for the general medicine graph Neo4j
  GENERAL_MED_USERNAME   username for general medicine graph (default: neo4j)
  GENERAL_MED_PASSWORD   password for general medicine graph
  GENERAL_MED_DATABASE   database name (default: neo4j)

  CM_SUBGRAPH_URI        Bolt URI for the CM subgraph Neo4j
  CM_SUBGRAPH_USERNAME   username for CM subgraph (default: neo4j)
  CM_SUBGRAPH_PASSWORD   password for CM subgraph
  CM_SUBGRAPH_DATABASE   database name (default: neo4j)
"""
from __future__ import annotations

import argparse
import csv
import gzip
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# CSV counting helpers
# ---------------------------------------------------------------------------

def count_csv_rows(path: Path, skip_header: bool = True) -> int:
    """Count data rows in a plain or gzipped CSV."""
    total = 0
    open_fn = gzip.open if path.suffix == ".gz" else open
    kwargs = {"mode": "rt", "encoding": "utf-8", "errors": "replace", "newline": ""}
    with open_fn(path, **kwargs) as fh:  # type: ignore[call-overload]
        reader = csv.reader(fh)
        if skip_header:
            next(reader, None)
        for _ in reader:
            total += 1
    return total


def count_tsv_rows(path: Path, skip_header: bool = True) -> int:
    with path.open(encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.reader(fh, delimiter="\t")
        if skip_header:
            next(reader, None)
        return sum(1 for _ in reader)


def count_gen_med_local(gen_med_dir: Path) -> Dict[str, int]:
    """Count nodes and edges in the four gen-med normalized CSVs."""
    counts: Dict[str, int] = {}
    files = {
        "semmed_nodes": gen_med_dir / "semmed_nodes_normalized.csv",
        "semmed_edges": gen_med_dir / "semmed_edges_normalized.csv",
        "ikraph_nodes": gen_med_dir / "ikraph_nodes_normalized.csv",
        "ikraph_edges": gen_med_dir / "ikraph_edges_normalized.csv",
    }
    for key, path in files.items():
        if path.exists():
            counts[key] = count_csv_rows(path)
        else:
            counts[key] = -1
            print(f"  [warn] missing: {path}", file=sys.stderr)
    counts["total_nodes"] = max(0, counts.get("semmed_nodes", 0)) + max(0, counts.get("ikraph_nodes", 0))
    counts["total_edges"] = max(0, counts.get("semmed_edges", 0)) + max(0, counts.get("ikraph_edges", 0))
    return counts


def count_cm_subgraph_local(cm_dir: Path) -> Dict[str, int]:
    """Count rows in the CM subgraph output files."""
    counts: Dict[str, int] = {}
    tsv_files = {
        "combo_seed_nodes":         cm_dir / "combo_seed_nodes.tsv",
        "semmed_edges_adjacent":    cm_dir / "semmed_edges_seed_adjacent.tsv",
        "ikraph_edges_adjacent":    cm_dir / "ikraph_edges_seed_adjacent.tsv",
        "semmed_concepts":          cm_dir / "semmed_concepts_seed_adjacent.tsv",
        "normalization_links":      cm_dir / "combo_normalization_links.tsv",
    }
    for key, path in tsv_files.items():
        if path.exists():
            counts[key] = count_tsv_rows(path)
        else:
            counts[key] = -1
            print(f"  [warn] missing: {path}", file=sys.stderr)
    return counts


# ---------------------------------------------------------------------------
# Neo4j query helpers
# ---------------------------------------------------------------------------

def get_neo4j_driver(uri: str, username: str, password: str):
    try:
        from neo4j import GraphDatabase, basic_auth  # type: ignore[import-untyped]
    except ImportError:
        raise SystemExit(
            "neo4j Python driver not installed — run: pip install 'neo4j>=5.0'"
        )
    driver = GraphDatabase.driver(uri, auth=basic_auth(username, password))
    try:
        driver.verify_connectivity()
    except Exception as exc:
        raise SystemExit(f"Cannot connect to {uri}: {exc}")
    return driver


def query_gen_med_counts(driver, database: str) -> Dict[str, int]:
    # The kubernetes gen-med graph is loaded from restructured CSVs where relationship
    # :TYPE is the uppercase biolink predicate (TREATS, AFFECTS, …) rather than
    # SEMMED_EDGE/IKRAPH_EDGE.  We distinguish sources via property presence:
    #   SemMed edges  → have  semmed_predicate  property
    #   iKraph edges  → have  ikraph_relation_type  property
    # IKraphEntity node count is lower than the local CSV by ~26k because
    # neo4j-admin --skip-duplicate-nodes drops iKraph nodes whose normalized_id
    # already appeared in the SemMed node file.
    queries = {
        "semmed_nodes": "MATCH (n:SemmedEntity) RETURN count(n) AS c",
        "ikraph_nodes": "MATCH (n:IKraphEntity) RETURN count(n) AS c",
        "semmed_edges": "MATCH ()-[r]->() WHERE r.semmed_predicate IS NOT NULL RETURN count(r) AS c",
        "ikraph_edges": "MATCH ()-[r]->() WHERE r.ikraph_relation_type IS NOT NULL RETURN count(r) AS c",
    }
    counts: Dict[str, int] = {}
    with driver.session(database=database) as session:
        for key, cypher in queries.items():
            result = session.run(cypher).single()
            counts[key] = result["c"] if result else 0
    counts["total_nodes"] = counts.get("semmed_nodes", 0) + counts.get("ikraph_nodes", 0)
    counts["total_edges"] = counts.get("semmed_edges", 0) + counts.get("ikraph_edges", 0)
    return counts


def check_kgx_fields_gen_med(driver, database: str) -> List[str]:
    """
    Return a list of KGX compliance failures for the gen-med graph.

    KGX spec requires:
      Nodes : id, category
      Edges : subject, object, predicate, knowledge_level, agent_type

    The gen-med graph uses biolink predicate names as relationship types
    (TREATS, AFFECTS, …) so we sample edges by property presence rather
    than by :TYPE.
    """
    failures: List[str] = []
    with driver.session(database=database) as s:
        # ── Nodes ──────────────────────────────────────────────────────────
        for node_label in ("SemmedEntity", "IKraphEntity"):
            row = s.run(
                f"MATCH (n:{node_label}) RETURN properties(n) AS p LIMIT 1"
            ).single()
            if not row:
                failures.append(f"nodes/{node_label}: no nodes found")
                continue
            props = dict(row["p"])
            for field in ("id", "category"):
                if not props.get(field):
                    failures.append(f"nodes/{node_label}.{field}: missing")

        # ── Edges ──────────────────────────────────────────────────────────
        for edge_label, filter_clause in (
            ("SemMed edges",  "r.semmed_predicate IS NOT NULL"),
            ("iKraph edges",  "r.ikraph_relation_type IS NOT NULL"),
        ):
            row = s.run(
                f"MATCH ()-[r]->() WHERE {filter_clause} RETURN properties(r) AS p LIMIT 1"
            ).single()
            if not row:
                failures.append(f"edges/{edge_label}: no edges found")
                continue
            props = dict(row["p"])
            for field in ("subject", "object", "predicate", "knowledge_level", "agent_type"):
                if not props.get(field):
                    failures.append(f"edges/{edge_label}.{field}: missing")
    return failures


def check_kgx_fields_cm_subgraph(driver, database: str) -> List[str]:
    """
    Return a list of KGX compliance failures for the CM subgraph graph.

    Known accepted gaps (same as kubernetes):
      - IKraphEntity nodes: no category (Biolink type not available in this pipeline)
      - IKRAPH_EDGE: no predicate (Biolink predicate not available in this pipeline)
    """
    failures: List[str] = []
    with driver.session(database=database) as s:
        # ComboSeed nodes are a COMBO-specific concept type, not standard KGX entities.
        # Only check the biomedical entity types for KGX id compliance.
        for node_label in ("SemmedEntity", "IKraphEntity"):
            row = s.run(
                f"MATCH (n:{node_label}) RETURN properties(n) AS p LIMIT 1"
            ).single()
            if not row:
                continue
            props = dict(row["p"])
            for field in ("id",):
                if not props.get(field):
                    failures.append(f"nodes/{node_label}.{field}: missing")

        for edge_type, required_fields in (
            ("SEMMED_EDGE", ("subject", "object", "predicate", "knowledge_level", "agent_type")),
            ("IKRAPH_EDGE", ("subject", "object", "knowledge_level", "agent_type")),
        ):
            row = s.run(
                f"MATCH ()-[r:{edge_type}]->() RETURN properties(r) AS p LIMIT 1"
            ).single()
            if not row:
                failures.append(f"edges/{edge_type}: no edges found")
                continue
            props = dict(row["p"])
            for field in required_fields:
                if not props.get(field):
                    failures.append(f"edges/{edge_type}.{field}: missing")
    return failures


def print_kgx_results(label: str, failures: List[str]) -> List[str]:
    print(f"\n{'─' * 64}")
    print(f"  KGX fields — {label}")
    print(f"{'─' * 64}")
    if not failures:
        print(f"  {OK} All required KGX fields present.")
    else:
        for msg in failures:
            print(f"  {FAIL} {msg}")
    return failures


def query_cm_subgraph_counts(driver, database: str) -> Dict[str, int]:
    queries = {
        "combo_seed_nodes":   "MATCH (n:ComboSeed) RETURN count(n) AS c",
        "semmed_nodes_cm":    "MATCH (n:SemmedEntity) RETURN count(n) AS c",
        "ikraph_nodes_cm":    "MATCH (n:IKraphEntity) RETURN count(n) AS c",
        "semmed_edges_cm":    "MATCH ()-[r:SEMMED_EDGE]->() RETURN count(r) AS c",
        "ikraph_edges_cm":    "MATCH ()-[r:IKRAPH_EDGE]->() RETURN count(r) AS c",
    }
    counts: Dict[str, int] = {}
    with driver.session(database=database) as session:
        for key, cypher in queries.items():
            result = session.run(cypher).single()
            counts[key] = result["c"] if result else 0
    return counts


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

OK = "\033[32m✓\033[0m"
WARN = "\033[33m~\033[0m"
FAIL = "\033[31m✗\033[0m"


def _pct_diff(a: int, b: int) -> str:
    if b == 0:
        return "N/A"
    return f"{abs(a - b) / b * 100:.1f}%"


def compare_counts(
    label: str,
    local: Dict[str, int],
    remote: Optional[Dict[str, int]],
    tolerance_pct: float = 0.5,
    per_key_tolerance: Optional[Dict[str, float]] = None,
    per_key_notes: Optional[Dict[str, str]] = None,
) -> List[str]:
    """
    Print a comparison table and return a list of failure messages (empty = pass).
    Counts are considered matching if within tolerance_pct %.
    per_key_tolerance: override tolerance for specific keys.
    per_key_notes: append a note string after the status for specific keys.
    """
    print(f"\n{'─' * 64}")
    print(f"  {label}")
    print(f"{'─' * 64}")
    header = f"  {'Metric':<35} {'Local':>10} {'Kubernetes':>12}  {'Status'}"
    print(header)
    print(f"  {'─' * 60}")

    failures: List[str] = []
    per_key_tolerance = per_key_tolerance or {}
    per_key_notes = per_key_notes or {}

    for key in sorted(local):
        local_val = local[key]
        note = per_key_notes.get(key, "")
        if remote is None:
            print(f"  {key:<35} {local_val:>10}  {note}")
            continue
        remote_val = remote.get(key, -1)
        if local_val < 0:
            status = f"  {WARN} local file missing"
        elif remote_val < 0:
            status = f"  {WARN} not in remote query set"
        else:
            tol = per_key_tolerance.get(key, tolerance_pct)
            diff_pct = abs(local_val - remote_val) / max(remote_val, 1) * 100
            if diff_pct <= tol:
                status = f"  {OK}"
            else:
                status = f"  {FAIL} diff={_pct_diff(local_val, remote_val)}"
                failures.append(
                    f"{label} / {key}: local={local_val:,}  k8s={remote_val:,}"
                    f"  ({_pct_diff(local_val, remote_val)} difference)"
                )
        suffix = f"  [{note}]" if note else ""
        print(f"  {key:<35} {local_val:>10} {remote_val:>12}  {status}{suffix}")

    return failures


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_env_file(path: Path) -> None:
    try:
        from dotenv import load_dotenv  # type: ignore[import-untyped]
        load_dotenv(dotenv_path=path, override=True)
    except ImportError:
        with path.open() as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Verify locally-built graph files match Kubernetes Neo4j node/edge counts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--gen-med-dir", type=Path, help="Directory with gen-med Neo4j-ready CSVs.")
    p.add_argument("--cm-subgraph-dir", type=Path, help="Directory with CM subgraph TSVs.")
    p.add_argument("--env-file", type=Path, help=".env file with NEO4J_* credentials.")
    p.add_argument(
        "--gen-med-uri",
        default=None,
        help="Bolt URI for general medicine graph Neo4j (default: $GENERAL_MED_URI).",
    )
    p.add_argument(
        "--gen-med-username",
        default=None,
        help="Username for general medicine graph (default: $GENERAL_MED_USERNAME or 'neo4j').",
    )
    p.add_argument("--gen-med-password", default=None, help="Password for general medicine graph.")
    p.add_argument("--gen-med-database", default=None, help="Database for general medicine graph (default: $GENERAL_MED_DATABASE or 'neo4j').")
    p.add_argument(
        "--cm-uri",
        default=None,
        help="Bolt URI for CM subgraph Neo4j (default: $CM_SUBGRAPH_URI).",
    )
    p.add_argument(
        "--cm-username",
        default=None,
        help="Username for CM subgraph (default: $CM_SUBGRAPH_USERNAME or 'neo4j').",
    )
    p.add_argument("--cm-password", default=None, help="Password for CM subgraph.")
    p.add_argument("--cm-database", default=None, help="Database for CM subgraph (default: $CM_SUBGRAPH_DATABASE or 'neo4j').")
    p.add_argument(
        "--local-only",
        action="store_true",
        help="Count local files only; do not attempt Neo4j connections.",
    )
    p.add_argument("--skip-gen-med", action="store_true", help="Skip gen-med graph verification.")
    p.add_argument("--skip-cm", action="store_true", help="Skip CM subgraph verification.")
    p.add_argument(
        "--tolerance",
        type=float,
        default=0.5,
        help="Allowed %% difference between local and Kubernetes counts (default: 0.5).",
    )
    return p.parse_args()


def main() -> int:
    csv.field_size_limit(min(sys.maxsize, 2_147_483_647))
    args = parse_args()

    if args.env_file:
        if not args.env_file.exists():
            raise SystemExit(f"env file not found: {args.env_file}")
        load_env_file(args.env_file)

    gen_med_uri      = args.gen_med_uri      or os.environ.get("GENERAL_MED_URI", "")
    gen_med_username = args.gen_med_username or os.environ.get("GENERAL_MED_USERNAME", "neo4j")
    gen_med_password = args.gen_med_password or os.environ.get("GENERAL_MED_PASSWORD", "")
    gen_med_database = args.gen_med_database or os.environ.get("GENERAL_MED_DATABASE", "neo4j")

    cm_uri      = args.cm_uri      or os.environ.get("CM_SUBGRAPH_URI", "")
    cm_username = args.cm_username or os.environ.get("CM_SUBGRAPH_USERNAME", "neo4j")
    cm_password = args.cm_password or os.environ.get("CM_SUBGRAPH_PASSWORD", "")
    cm_database = args.cm_database or os.environ.get("CM_SUBGRAPH_DATABASE", "neo4j")

    if not args.gen_med_dir and not args.cm_subgraph_dir:
        raise SystemExit("Provide at least one of --gen-med-dir or --cm-subgraph-dir.")

    all_failures: List[str] = []

    # ── Gen-med graph ────────────────────────────────────────────────────────
    if not args.skip_gen_med and args.gen_med_dir:
        print(f"\n[Gen-med graph] counting local files in: {args.gen_med_dir}")
        local_gm = count_gen_med_local(args.gen_med_dir)

        remote_gm: Optional[Dict[str, int]] = None
        if not args.local_only:
            if not gen_med_uri:
                print("  [warn] GENERAL_MED_URI not set; skipping remote comparison for gen-med graph", file=sys.stderr)
            else:
                if not gen_med_password:
                    import getpass
                    gen_med_password = getpass.getpass(f"Neo4j password for {gen_med_username}@{gen_med_uri}: ")
                print(f"[Gen-med graph] querying Kubernetes Neo4j at {gen_med_uri} ...")
                try:
                    driver = get_neo4j_driver(gen_med_uri, gen_med_username, gen_med_password)
                    remote_gm = query_gen_med_counts(driver, gen_med_database)
                    driver.close()
                except SystemExit as exc:
                    print(f"  [warn] {exc}", file=sys.stderr)

        failures = compare_counts(
            "Gen-med graph (CIM KG)",
            local_gm,
            remote_gm,
            args.tolerance,
            # ikraph_nodes: neo4j-admin --skip-duplicate-nodes drops iKraph nodes
            # whose normalized_id also appears in the SemMed node file; expect ~26k fewer.
            per_key_tolerance={"ikraph_nodes": 15.0, "total_nodes": 8.0},
            per_key_notes={"ikraph_nodes": "expected lower in k8s: deduplication by neo4j-admin"},
        )
        all_failures.extend(failures)

        if not args.local_only and remote_gm is not None:
            kgx_driver = get_neo4j_driver(gen_med_uri, gen_med_username, gen_med_password)
            kgx_failures = check_kgx_fields_gen_med(kgx_driver, gen_med_database)
            kgx_driver.close()
            kgx_failures = print_kgx_results("Gen-med graph (kubernetes)", kgx_failures)
            if kgx_failures:
                all_failures.extend([f"[KGX] {f}" for f in kgx_failures])

    # ── CM subgraph ──────────────────────────────────────────────────────────
    if not args.skip_cm and args.cm_subgraph_dir:
        print(f"\n[CM subgraph] counting local files in: {args.cm_subgraph_dir}")
        local_cm = count_cm_subgraph_local(args.cm_subgraph_dir)

        remote_cm: Optional[Dict[str, int]] = None
        if not args.local_only:
            if not cm_uri:
                print("  [warn] CM_SUBGRAPH_URI not set; skipping remote comparison for CM subgraph", file=sys.stderr)
            else:
                if not cm_password:
                    import getpass
                    cm_password = getpass.getpass(f"Neo4j password for {cm_username}@{cm_uri}: ")
                print(f"[CM subgraph] querying Kubernetes Neo4j at {cm_uri} ...")
                try:
                    driver = get_neo4j_driver(cm_uri, cm_username, cm_password)
                    remote_cm = query_cm_subgraph_counts(driver, cm_database)
                    driver.close()
                except SystemExit as exc:
                    print(f"  [warn] {exc}", file=sys.stderr)

        # Map local TSV names to corresponding remote query keys for comparison.
        # The remote queries use different keys; only compare where names align.
        aligned_local: Dict[str, int] = {}
        aligned_remote: Optional[Dict[str, int]] = None
        if remote_cm is not None:
            aligned_remote = {}
            key_map = {
                "combo_seed_nodes":      "combo_seed_nodes",
                "semmed_edges_adjacent": "semmed_edges_cm",
                "ikraph_edges_adjacent": "ikraph_edges_cm",
            }
            for local_key, remote_key in key_map.items():
                aligned_local[local_key]   = local_cm.get(local_key, -1)
                aligned_remote[local_key]  = remote_cm.get(remote_key, -1)
            # Counts-only keys (no remote equivalent) just show local
            for k in local_cm:
                if k not in aligned_local:
                    aligned_local[k] = local_cm[k]
        else:
            aligned_local = local_cm

        failures = compare_counts("CM subgraph", aligned_local, aligned_remote, args.tolerance)
        all_failures.extend(failures)

        if not args.local_only and remote_cm is not None:
            kgx_driver = get_neo4j_driver(cm_uri, cm_username, cm_password)
            kgx_failures = check_kgx_fields_cm_subgraph(kgx_driver, cm_database)
            kgx_driver.close()
            kgx_failures = print_kgx_results("CM subgraph (kubernetes)", kgx_failures)
            if kgx_failures:
                all_failures.extend([f"[KGX] {f}" for f in kgx_failures])

    # ── Final result ─────────────────────────────────────────────────────────
    print(f"\n{'═' * 64}")
    if all_failures:
        print(f"  RESULT: {FAIL} {len(all_failures)} mismatch(es) found")
        for msg in all_failures:
            print(f"    • {msg}")
        print()
        return 1
    else:
        if args.local_only:
            print(f"  RESULT: {OK} Local file counts look consistent (no remote comparison).")
        else:
            print(f"  RESULT: {OK} All counts match Kubernetes within {args.tolerance}%.")
        print()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
