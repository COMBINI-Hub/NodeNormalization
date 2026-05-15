#!/usr/bin/env python3
"""
build_pipeline.py — KG build pipeline orchestrator

Runs all steps from raw iKraph/SemMedDB source files to Neo4j-import-ready CSVs and
the CM subgraph, in the correct order.  Each step can also be called individually by
importing it from scripts/pipeline_steps/.

Usage (full pipeline):
    python3 scripts/build_pipeline.py \\
        --semmed-dir        /path/to/semmed_data \\
        --ikraph-dir        /path/to/iKraph_raw/iKraph_full \\
        --reltype-xlsx      ikraph_reltype_to_biolink_mapping_review.xlsx \\
        --combo-owl         /path/to/COMBO.owl \\
        --norm-dir          semmed_ikraph_normalized \\
        --output-dir        /tmp/kg_build_out

Skip steps:
    --skip-gen-med        skip Steps 0-4 (general med KG)
    --skip-cm-subgraph    skip Steps 5-6 (CM subgraph)
    --force-normalize     re-run node normalization even if JSON outputs exist

Pipeline steps
  Step 0  generate_reltype_map      xlsx → ikraph_reltype_map.csv
  Step 1  preprocess_raw_edges      DBRelations.json + connections.csv → cleaned CSVs
  Step 2  normalize_nodes           CURIEs → NodeNorm JSON (skipped if files exist)
  Step 3  build_normalized_import_csvs  norm JSONs + raw edges → 4 CSVs (gen_med_graph/)
  Step 4  restructure_for_neo4j     set :LABEL/:TYPE → gen_med_graph_neo4j/
  Step 5  extract_cm_subgraph       COMBO OWL + norm JSONs → cm_subgraph/ TSVs
  Step 6  build_cm_concept_table    SemMed edges + norm JSON → semmed_concepts TSV
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

# Step modules
from pipeline_steps.reltype_map import generate_reltype_map, load_reltype_map
from pipeline_steps.preprocess import preprocess_ikraph_edges, preprocess_semmed_edges
from pipeline_steps.normalize import normalize_nodes
from pipeline_steps.build_import_csvs import build_normalized_import_csvs
from pipeline_steps.restructure import restructure_for_neo4j
from pipeline_steps.extract_cm import extract_cm_subgraph
from pipeline_steps.cm_concepts import build_cm_concept_table


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass
class PipelineConfig:
    # Raw inputs
    semmed_dir: Path          # contains predication.csv.gz, concept.csv, connections.csv
    ikraph_dir: Path          # iKraph_raw/iKraph_full/: DBRelations.json, NER_ID_dict_cap_final.json, etc.
    reltype_xlsx: Path        # ikraph_reltype_to_biolink_mapping_review.xlsx
    combo_owl: Path           # COMBO ontology .owl file

    # Where to read/write normalization artifacts
    norm_dir: Path            # semmed_ikraph_normalized/

    # Root output directory
    output_dir: Path

    # Optional extras forwarded to extract_cm_subgraph
    semmed_connections_csv: Optional[Path] = None
    embedding_best: Optional[Path] = None
    ikraph_names_flat: Optional[Path] = None
    denylist_file: Optional[Path] = None
    cm_extra_args: List[str] = field(default_factory=list)

    # Flags
    skip_gen_med: bool = False
    skip_cm_subgraph: bool = False
    force_normalize: bool = False
    stream_ikraph_json: bool = False
    nodenorm_endpoint: str = "http://127.0.0.1:8000/get_normalized_nodes"

    # ── Derived paths (properties so they update if fields change) ──────────

    @property
    def predication_path(self) -> Path:
        gz = self.semmed_dir / "predication.csv.gz"
        return gz if gz.exists() else self.semmed_dir / "predication.csv"

    @property
    def semmed_concepts_csv(self) -> Path:
        return self.semmed_dir / "concept.csv"

    @property
    def connections_csv(self) -> Path:
        return self.semmed_dir / "connections.csv"

    @property
    def db_relations_json(self) -> Path:
        return self.ikraph_dir / "DBRelations.json"

    @property
    def ner_json(self) -> Path:
        return self.ikraph_dir / "NER_ID_dict_cap_final.json"

    @property
    def pubmed_json(self) -> Path:
        return self.ikraph_dir / "PubMedList.json"

    @property
    def reltypeint_json(self) -> Path:
        return self.ikraph_dir / "RelTypeInt.json"

    @property
    def reltype_map_csv(self) -> Path:
        return self.norm_dir / "ikraph_reltype_map.csv"

    @property
    def semmed_normalized_json(self) -> Path:
        return self.norm_dir / "semmed_normalized_full.json"

    @property
    def ikraph_normalized_json(self) -> Path:
        return self.norm_dir / "ikraph_normalized_full.json"

    @property
    def ikraph_edges_cleaned(self) -> Path:
        return self.norm_dir / "ikraph_edges_cleaned.csv"

    @property
    def gen_med_dir(self) -> Path:
        return self.output_dir / "gen_med_graph"

    @property
    def gen_med_neo4j_dir(self) -> Path:
        return self.output_dir / "gen_med_graph_neo4j"

    @property
    def cm_subgraph_dir(self) -> Path:
        return self.output_dir / "cm_subgraph"


@dataclass
class StepResult:
    name: str
    output_paths: List[Path] = field(default_factory=list)
    skipped: bool = False
    elapsed_s: float = 0.0
    message: str = ""


class PipelineStepError(RuntimeError):
    """Raised when a pipeline step fails."""


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_pipeline(config: PipelineConfig) -> List[StepResult]:
    """
    Execute all enabled pipeline steps in order.  Returns one StepResult per
    step that was attempted (skipped steps are included with skipped=True).
    """
    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.norm_dir.mkdir(parents=True, exist_ok=True)

    results: List[StepResult] = []

    def _run(name: str, fn, *args, **kwargs) -> StepResult:
        print(f"\n{'═'*60}", file=sys.stderr)
        print(f"  {name}", file=sys.stderr)
        print(f"{'═'*60}", file=sys.stderr)
        t0 = time.perf_counter()
        try:
            output = fn(*args, **kwargs)
        except Exception as exc:
            raise PipelineStepError(f"Step '{name}' failed: {exc}") from exc
        elapsed = time.perf_counter() - t0
        paths = output if isinstance(output, list) else ([output] if output else [])
        r = StepResult(name=name, output_paths=paths, elapsed_s=elapsed)
        results.append(r)
        print(f"  ✓ {name} completed in {elapsed:.1f}s", file=sys.stderr)
        return r

    def _skip(name: str) -> StepResult:
        r = StepResult(name=name, skipped=True, message="skipped by flag")
        results.append(r)
        print(f"\n[pipeline] skipping: {name}", file=sys.stderr)
        return r

    # ── Step 0: reltype map ─────────────────────────────────────────────────
    if not config.skip_gen_med:
        _run(
            "Step 0 — generate_reltype_map",
            generate_reltype_map,
            config.reltype_xlsx,
            config.reltypeint_json,
            config.reltype_map_csv,
        )
    else:
        _skip("Step 0 — generate_reltype_map")

    # ── Step 1: preprocess raw edges ────────────────────────────────────────
    if not config.skip_gen_med:
        _run(
            "Step 1a — preprocess iKraph edges",
            preprocess_ikraph_edges,
            config.db_relations_json,
            config.norm_dir / "ikraph_edges_cleaned.csv",
        )
        _run(
            "Step 1b — preprocess SemMed edges",
            preprocess_semmed_edges,
            config.connections_csv,
            config.norm_dir / "semmeddb_edges_cleaned.csv",
        )
    else:
        _skip("Step 1 — preprocess_raw_edges")

    # ── Step 2: node normalization ──────────────────────────────────────────
    if not config.skip_gen_med and not config.skip_cm_subgraph:
        t0 = time.perf_counter()
        semmed_json, ikraph_json = normalize_nodes(
            config.predication_path,
            config.ner_json,
            config.norm_dir,
            endpoint=config.nodenorm_endpoint,
            force=config.force_normalize,
        )
        elapsed = time.perf_counter() - t0
        r = StepResult(
            name="Step 2 — normalize_nodes",
            output_paths=[semmed_json, ikraph_json],
            elapsed_s=elapsed,
            skipped=(not config.force_normalize and semmed_json.exists() and ikraph_json.exists()),
        )
        results.append(r)
    else:
        _skip("Step 2 — normalize_nodes")

    # ── Step 3: build import CSVs ───────────────────────────────────────────
    if not config.skip_gen_med:
        reltype_map = load_reltype_map(config.reltype_map_csv)
        _run(
            "Step 3 — build_normalized_import_csvs",
            build_normalized_import_csvs,
            config.semmed_normalized_json,
            config.ikraph_normalized_json,
            config.predication_path,
            config.ner_json,
            config.db_relations_json,
            config.pubmed_json,
            reltype_map,
            config.gen_med_dir,
            stream_ikraph_json=config.stream_ikraph_json,
        )
    else:
        _skip("Step 3 — build_normalized_import_csvs")

    # ── Step 4: restructure for Neo4j ───────────────────────────────────────
    if not config.skip_gen_med:
        _run(
            "Step 4 — restructure_for_neo4j",
            restructure_for_neo4j,
            config.gen_med_dir,
            config.gen_med_neo4j_dir,
        )
    else:
        _skip("Step 4 — restructure_for_neo4j")

    # ── Step 5: extract CM subgraph ─────────────────────────────────────────
    if not config.skip_cm_subgraph:
        _run(
            "Step 5 — extract_cm_subgraph",
            extract_cm_subgraph,
            config.semmed_normalized_json,
            config.ikraph_normalized_json,
            config.semmed_concepts_csv,
            config.ikraph_edges_cleaned,
            config.combo_owl,
            config.cm_subgraph_dir,
            ikraph_nodes_json=config.ner_json,
            semmed_connections_csv=config.semmed_connections_csv,
            embedding_best=config.embedding_best,
            ikraph_names_flat=config.ikraph_names_flat,
            denylist_file=config.denylist_file,
            extra_args=config.cm_extra_args if config.cm_extra_args else None,
        )
    else:
        _skip("Step 5 — extract_cm_subgraph")

    # ── Step 6: CM concept table ────────────────────────────────────────────
    if not config.skip_cm_subgraph:
        seeds_and_1hop = config.cm_subgraph_dir / "semmed_edges_combo_seeds_and_1hop.tsv"
        _run(
            "Step 6 — build_cm_concept_table",
            build_cm_concept_table,
            seeds_and_1hop,
            config.semmed_normalized_json,
            config.semmed_concepts_csv,
            config.cm_subgraph_dir / "semmed_concepts_combo_seeds_and_1hop.tsv",
        )
    else:
        _skip("Step 6 — build_cm_concept_table")

    # ── Summary ─────────────────────────────────────────────────────────────
    _print_summary(results, config)
    return results


def _print_summary(results: List[StepResult], config: PipelineConfig) -> None:
    print(f"\n{'═'*60}", file=sys.stderr)
    print("  Pipeline summary", file=sys.stderr)
    print(f"{'═'*60}", file=sys.stderr)
    total = sum(r.elapsed_s for r in results)
    for r in results:
        status = "SKIP" if r.skipped else f"{r.elapsed_s:5.1f}s"
        print(f"  [{status:>6}]  {r.name}", file=sys.stderr)
    print(f"\n  Total elapsed: {total:.1f}s", file=sys.stderr)
    if not config.skip_gen_med:
        print(f"\n  Gen-med graph (Neo4j-ready): {config.gen_med_neo4j_dir}", file=sys.stderr)
        print(
            "  → load with: combini-kubernetes/neo4j/load-semmed-ikraph-to-new-instance.sh"
            f" (USE_NORMALIZED=1 NORMALIZED_DIR={config.gen_med_neo4j_dir})",
            file=sys.stderr,
        )
    if not config.skip_cm_subgraph:
        print(f"\n  CM subgraph: {config.cm_subgraph_dir}", file=sys.stderr)
        print(
            "  → load with: combini-kubernetes/neo4j/load-cm-subgraph-to-k8s.sh",
            file=sys.stderr,
        )
    print(f"\n  Verify counts with:", file=sys.stderr)
    print(
        f"  python3 scripts/verify_graph_match.py"
        f" --gen-med-dir {config.gen_med_neo4j_dir}"
        f" --cm-subgraph-dir {config.cm_subgraph_dir}"
        f" --env-file .env",
        file=sys.stderr,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Build the General Med KG and CM Subgraph from raw source files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    # Required
    p.add_argument("--semmed-dir", type=Path, required=True,
                   help="semmed_data/ directory (predication.csv.gz, concept.csv, connections.csv)")
    p.add_argument("--ikraph-dir", type=Path, required=True,
                   help="iKraph_raw/iKraph_full/ directory")
    p.add_argument("--reltype-xlsx", type=Path, required=True,
                   help="ikraph_reltype_to_biolink_mapping_review.xlsx")
    p.add_argument("--combo-owl", type=Path, required=True,
                   help="COMBO ontology .owl file")
    p.add_argument("--norm-dir", type=Path, required=True,
                   help="Directory holding (or to receive) normalization JSON files")
    p.add_argument("--output-dir", type=Path, required=True,
                   help="Root output directory for gen_med_graph/ and cm_subgraph/")

    # Optional inputs
    p.add_argument("--semmed-connections-csv", type=Path, default=None)
    p.add_argument("--embedding-best", type=Path, default=None)
    p.add_argument("--ikraph-names-flat", type=Path, default=None)
    p.add_argument("--denylist-file", type=Path, default=None)
    p.add_argument("--cm-extra-args", nargs=argparse.REMAINDER, default=[],
                   help="Extra arguments forwarded verbatim to extract_combo_semmed_ikraph_subgraph.py")

    # Flags
    p.add_argument("--skip-gen-med", action="store_true",
                   help="Skip Steps 0-4 (general med graph)")
    p.add_argument("--skip-cm-subgraph", action="store_true",
                   help="Skip Steps 5-6 (CM subgraph)")
    p.add_argument("--force-normalize", action="store_true",
                   help="Re-run node normalization even if JSON outputs already exist")
    p.add_argument("--stream-ikraph-json", action="store_true",
                   help="Stream iKraph normalization JSON via ijson (saves RAM; requires ijson)")
    p.add_argument("--nodenorm-endpoint",
                   default="http://127.0.0.1:8000/get_normalized_nodes",
                   help="NodeNorm API endpoint for the normalize step")
    return p


def main() -> int:
    args = _build_parser().parse_args()

    config = PipelineConfig(
        semmed_dir=args.semmed_dir,
        ikraph_dir=args.ikraph_dir,
        reltype_xlsx=args.reltype_xlsx,
        combo_owl=args.combo_owl,
        norm_dir=args.norm_dir,
        output_dir=args.output_dir,
        semmed_connections_csv=args.semmed_connections_csv,
        embedding_best=args.embedding_best,
        ikraph_names_flat=args.ikraph_names_flat,
        denylist_file=args.denylist_file,
        cm_extra_args=args.cm_extra_args,
        skip_gen_med=args.skip_gen_med,
        skip_cm_subgraph=args.skip_cm_subgraph,
        force_normalize=args.force_normalize,
        stream_ikraph_json=args.stream_ikraph_json,
        nodenorm_endpoint=args.nodenorm_endpoint,
    )

    try:
        run_pipeline(config)
    except PipelineStepError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    # Allow running from repo root as: python3 scripts/build_pipeline.py ...
    import os
    sys.path.insert(0, os.path.dirname(__file__))
    raise SystemExit(main())
