#!/usr/bin/env bash
# Build both the general medicine graph (CIM KG / SemMed+iKraph normalized) and
# the CM subgraph from raw input files.
#
# Outputs
#   <output-dir>/gen_med_graph/          — Neo4j-import-ready normalized CSVs
#   <output-dir>/gen_med_graph_neo4j/    — Restructured CSVs with :LABEL / :TYPE columns
#   <output-dir>/cm_subgraph/            — CM subgraph TSVs for loading into Neo4j
#
# Usage
#   ./scripts/build_graphs_from_raw.sh [options]
#
# Required options (no defaults):
#   --semmed-nodes            CSV mapping semmed_id → normalized_id (labels, types)
#   --semmed-predication      predication.csv.gz from SemMedDB
#   --semmed-concepts-csv     concept.csv from SemMedDB
#   --ikraph-nodes-json       NER_ID_dict_cap_final.json from iKraph_raw
#   --ikraph-db-json          iKraph_full/DBRelations.json from iKraph_raw
#   --ikraph-pubmed-json      iKraph_full/PubMedList.json from iKraph_raw
#   --ikraph-normalized-json  ikraph normalization JSON (NodeNorm output)
#   --ikraph-reltype-map      CSV: int_rep → proposed_biolink_predicate (from generate_ikraph_reltype_map.py)
#   --ikraph-edges-cleaned    ikraph_edges_cleaned.csv (for CM subgraph)
#   --semmed-normalized-json  semmed_normalized_full_with_pubchem.json
#   --combo-owl               COMBO ontology OWL file
#   --output-dir              root output directory (created if absent)
#
# Optional:
#   --skip-gen-med            skip the general medicine graph steps
#   --skip-cm-subgraph        skip the CM subgraph steps
#   --stream-ikraph-json      pass --stream-ikraph-norm-json to build script (saves RAM)
#   --extract-args            extra args forwarded to extract_combo_semmed_ikraph_subgraph.py
#                             e.g. --extract-args='--focus-intervention-umls-only'
#   --semmed-connections-csv  connections.csv from SemMedDB (for extract step)
#   --embedding-best          combo_external_highsim_bestper_source.tsv (optional)
#   --ikraph-names-flat       ikraph_names_flat.tsv (optional)
#   --denylist-file           curated_false_matches.tsv (optional)
#
# Environment overrides (same as individual scripts):
#   COMBO_ROOT   — NodeNormalization root (default: directory containing this script's parent)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMBO_ROOT="${COMBO_ROOT:-$(dirname "$SCRIPT_DIR")}"

# ────────────────────────────────────────────────────────────────────────────
# Argument parsing
# ────────────────────────────────────────────────────────────────────────────
SEMMED_NODES=""
SEMMED_PREDICATION=""
SEMMED_CONCEPTS_CSV=""
IKRAPH_NODES_JSON=""
IKRAPH_DB_JSON=""
IKRAPH_PUBMED_JSON=""
IKRAPH_NORMALIZED_JSON=""
IKRAPH_RELTYPE_MAP=""
IKRAPH_EDGES_CLEANED=""
SEMMED_NORMALIZED_JSON=""
COMBO_OWL=""
OUTPUT_DIR=""

SKIP_GEN_MED=0
SKIP_CM_SUBGRAPH=0
STREAM_IKRAPH_JSON=0
EXTRACT_ARGS=""

# Optional paths with defaults
SEMMED_CONNECTIONS_CSV=""
EMBEDDING_BEST=""
IKRAPH_NAMES_FLAT=""
DENYLIST_FILE=""

usage() {
  grep '^#' "$0" | grep -v '^#!/' | sed 's/^# \?//'
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --semmed-nodes)               SEMMED_NODES="$2";              shift 2 ;;
    --semmed-predication)         SEMMED_PREDICATION="$2";        shift 2 ;;
    --semmed-concepts-csv)        SEMMED_CONCEPTS_CSV="$2";       shift 2 ;;
    --ikraph-nodes-json)          IKRAPH_NODES_JSON="$2";         shift 2 ;;
    --ikraph-db-json)             IKRAPH_DB_JSON="$2";            shift 2 ;;
    --ikraph-pubmed-json)         IKRAPH_PUBMED_JSON="$2";        shift 2 ;;
    --ikraph-normalized-json)     IKRAPH_NORMALIZED_JSON="$2";    shift 2 ;;
    --ikraph-reltype-map)         IKRAPH_RELTYPE_MAP="$2";        shift 2 ;;
    --ikraph-edges-cleaned)       IKRAPH_EDGES_CLEANED="$2";      shift 2 ;;
    --semmed-normalized-json)     SEMMED_NORMALIZED_JSON="$2";    shift 2 ;;
    --combo-owl)                  COMBO_OWL="$2";                 shift 2 ;;
    --output-dir)                 OUTPUT_DIR="$2";                shift 2 ;;
    --skip-gen-med)               SKIP_GEN_MED=1;                 shift ;;
    --skip-cm-subgraph)           SKIP_CM_SUBGRAPH=1;             shift ;;
    --stream-ikraph-json)         STREAM_IKRAPH_JSON=1;           shift ;;
    --extract-args)               EXTRACT_ARGS="$2";              shift 2 ;;
    --semmed-connections-csv)     SEMMED_CONNECTIONS_CSV="$2";    shift 2 ;;
    --embedding-best)             EMBEDDING_BEST="$2";            shift 2 ;;
    --ikraph-names-flat)          IKRAPH_NAMES_FLAT="$2";         shift 2 ;;
    --denylist-file)              DENYLIST_FILE="$2";             shift 2 ;;
    -h|--help)                    usage ;;
    *) echo "Unknown option: $1" >&2; usage ;;
  esac
done

# ────────────────────────────────────────────────────────────────────────────
# Validate required arguments
# ────────────────────────────────────────────────────────────────────────────
die() { echo "ERROR: $*" >&2; exit 1; }

[[ -n "$OUTPUT_DIR" ]] || die "--output-dir is required"

if [[ "$SKIP_GEN_MED" != "1" ]]; then
  [[ -n "$SEMMED_NODES" ]]           || die "--semmed-nodes is required for gen-med graph"
  [[ -n "$SEMMED_PREDICATION" ]]     || die "--semmed-predication is required for gen-med graph"
  [[ -n "$IKRAPH_NODES_JSON" ]]      || die "--ikraph-nodes-json is required for gen-med graph"
  [[ -n "$IKRAPH_DB_JSON" ]]         || die "--ikraph-db-json is required for gen-med graph"
  [[ -n "$IKRAPH_PUBMED_JSON" ]]     || die "--ikraph-pubmed-json is required for gen-med graph"
  [[ -n "$IKRAPH_NORMALIZED_JSON" ]] || die "--ikraph-normalized-json is required for gen-med graph"
  [[ -n "$IKRAPH_RELTYPE_MAP" ]]     || die "--ikraph-reltype-map is required for gen-med graph"
fi

if [[ "$SKIP_CM_SUBGRAPH" != "1" ]]; then
  [[ -n "$IKRAPH_EDGES_CLEANED" ]]      || die "--ikraph-edges-cleaned is required for CM subgraph"
  [[ -n "$SEMMED_NORMALIZED_JSON" ]]    || die "--semmed-normalized-json is required for CM subgraph"
  [[ -n "$SEMMED_CONCEPTS_CSV" ]]       || die "--semmed-concepts-csv is required for CM subgraph"
  [[ -n "$COMBO_OWL" ]]                 || die "--combo-owl is required for CM subgraph"
  [[ -n "$IKRAPH_NORMALIZED_JSON" ]]    || die "--ikraph-normalized-json is required for CM subgraph"
fi

# ────────────────────────────────────────────────────────────────────────────
# Paths derived from output dir
# ────────────────────────────────────────────────────────────────────────────
GEN_MED_DIR="$OUTPUT_DIR/gen_med_graph"
GEN_MED_NEO4J_DIR="$OUTPUT_DIR/gen_med_graph_neo4j"
CM_SUBGRAPH_DIR="$OUTPUT_DIR/cm_subgraph"

mkdir -p "$GEN_MED_DIR" "$GEN_MED_NEO4J_DIR" "$CM_SUBGRAPH_DIR"

# ────────────────────────────────────────────────────────────────────────────
# Helper: step header
# ────────────────────────────────────────────────────────────────────────────
step() {
  echo ""
  echo "════════════════════════════════════════════════════════════"
  echo "  $*"
  echo "════════════════════════════════════════════════════════════"
}

# ────────────────────────────────────────────────────────────────────────────
# PART A — General Medicine Graph (CIM KG / SemMed+iKraph normalized)
# ────────────────────────────────────────────────────────────────────────────
if [[ "$SKIP_GEN_MED" != "1" ]]; then

  step "[A1/2] Building normalized SemMed+iKraph import CSVs"
  build_args=(
    --semmed-nodes           "$SEMMED_NODES"
    --semmed-predication     "$SEMMED_PREDICATION"
    --ikraph-nodes-json      "$IKRAPH_NODES_JSON"
    --ikraph-db-json         "$IKRAPH_DB_JSON"
    --ikraph-pubmed-json     "$IKRAPH_PUBMED_JSON"
    --ikraph-normalized-json "$IKRAPH_NORMALIZED_JSON"
    --ikraph-reltype-map     "$IKRAPH_RELTYPE_MAP"
    --output-dir             "$GEN_MED_DIR"
  )
  if [[ "$STREAM_IKRAPH_JSON" == "1" ]]; then
    build_args+=(--stream-ikraph-norm-json)
  fi
  python3 "$SCRIPT_DIR/build_semmed_ikraph_normalized_import.py" "${build_args[@]}"

  step "[A2/2] Restructuring CSVs for Neo4j labels / relationship types"
  python3 "$SCRIPT_DIR/restructure_normalized_kg_csvs.py" \
    --input-dir  "$GEN_MED_DIR" \
    --output-dir "$GEN_MED_NEO4J_DIR"

  echo ""
  echo "General medicine graph outputs:"
  echo "  Raw normalized CSVs  : $GEN_MED_DIR"
  echo "  Neo4j-ready CSVs     : $GEN_MED_NEO4J_DIR"
  echo "  (semmed_nodes_normalized.csv, semmed_edges_normalized.csv,"
  echo "   ikraph_nodes_normalized.csv, ikraph_edges_normalized.csv)"
fi

# ────────────────────────────────────────────────────────────────────────────
# PART B — CM Subgraph
# ────────────────────────────────────────────────────────────────────────────
if [[ "$SKIP_CM_SUBGRAPH" != "1" ]]; then

  step "[B1/3] Extracting COMBO-matched seeds + 1-hop edges"
  extract_cmd=(
    python3 "$COMBO_ROOT/extract_combo_semmed_ikraph_subgraph.py"
    --semmed-normalized  "$SEMMED_NORMALIZED_JSON"
    --ikraph-normalized  "$IKRAPH_NORMALIZED_JSON"
    --semmed-concepts    "$SEMMED_CONCEPTS_CSV"
    --ikraph-edges       "$IKRAPH_EDGES_CLEANED"
    --combo-owl          "$COMBO_OWL"
    --output-dir         "$CM_SUBGRAPH_DIR"
  )
  if [[ -n "$IKRAPH_NODES_JSON" ]]; then
    extract_cmd+=(--ikraph-nodes "$IKRAPH_NODES_JSON")
  fi
  if [[ -n "$SEMMED_CONNECTIONS_CSV" ]]; then
    extract_cmd+=(--semmed-edges "$SEMMED_CONNECTIONS_CSV")
  fi
  if [[ -n "$EMBEDDING_BEST" ]]; then
    extract_cmd+=(--embedding-best "$EMBEDDING_BEST")
  fi
  if [[ -n "$IKRAPH_NAMES_FLAT" ]]; then
    extract_cmd+=(--ikraph-names-flat "$IKRAPH_NAMES_FLAT")
  fi
  if [[ -n "$DENYLIST_FILE" ]]; then
    extract_cmd+=(--denylist-file "$DENYLIST_FILE")
  fi
  if [[ -n "$EXTRACT_ARGS" ]]; then
    # shellcheck disable=SC2206
    extract_cmd+=($EXTRACT_ARGS)
  fi
  "${extract_cmd[@]}"

  step "[B2/3] Building SemMed concept/type table for extracted edges"
  python3 "$SCRIPT_DIR/build_semmed_concepts_from_normalized.py" \
    --semmed-edges-file    "$CM_SUBGRAPH_DIR/semmed_edges_seed_adjacent.tsv" \
    --normalized-json-file "$SEMMED_NORMALIZED_JSON" \
    --semmed-concepts-csv  "$SEMMED_CONCEPTS_CSV" \
    --output-file          "$CM_SUBGRAPH_DIR/semmed_concepts_seed_adjacent.tsv"

  step "[B3/3] CM subgraph ready"
  echo ""
  echo "CM subgraph outputs in: $CM_SUBGRAPH_DIR"
  echo "  combo_seed_nodes.tsv"
  echo "  combo_normalization_links.tsv"
  echo "  semmed_edges_seed_adjacent.tsv"
  echo "  ikraph_edges_seed_adjacent.tsv"
  echo "  semmed_concepts_seed_adjacent.tsv"
  echo "  combo_subgraph_summary.json"
  echo ""
  echo "To load into Kubernetes Neo4j:"
  echo "  cd /path/to/combini-kubernetes/neo4j"
  echo "  ./load-cm-subgraph-to-k8s.sh \\"
  echo "    \"$COMBO_ROOT\" \\"
  echo "    /path/to/neo4j-k8s.env"
fi

# ────────────────────────────────────────────────────────────────────────────
# Summary
# ────────────────────────────────────────────────────────────────────────────
step "Pipeline complete"
echo ""
if [[ "$SKIP_GEN_MED" != "1" ]]; then
  echo "  Gen med graph (Neo4j-ready):  $GEN_MED_NEO4J_DIR"
  echo "  To load into CIM KG Neo4j K8s, set USE_NORMALIZED=1 and NORMALIZED_DIR=$GEN_MED_NEO4J_DIR"
  echo "  then run: combini-kubernetes/neo4j/load-semmed-ikraph-to-new-instance.sh"
fi
if [[ "$SKIP_CM_SUBGRAPH" != "1" ]]; then
  echo "  CM subgraph:                  $CM_SUBGRAPH_DIR"
  echo "  To load into CM Neo4j K8s, run: combini-kubernetes/neo4j/load-cm-subgraph-to-k8s.sh"
fi
echo ""
echo "Verify graph counts + KGX field compliance against Kubernetes with:"
echo "  python3 $SCRIPT_DIR/verify_graph_match.py \\"
echo "    --gen-med-dir        \"$GEN_MED_NEO4J_DIR\" \\"
echo "    --cm-subgraph-dir    \"$CM_SUBGRAPH_DIR\" \\"
echo "    --env-file           /path/to/.env"
echo ""
echo "KGX compatibility notes:"
echo "  Gen-med graph: CSVs in gen_med_graph_neo4j/ already include all KGX-required"
echo "    edge fields (subject, object, predicate, knowledge_level, agent_type)."
echo "    Nodes include id and category. No post-import patching required."
echo "  CM subgraph: run patch_kgx_fields.py --graph cm-subgraph after loading to set"
echo "    SEMMED_EDGE subject/object/predicate/knowledge_level/agent_type."
echo "    IKRAPH_EDGE predicate is not available in that pipeline (known gap)."
echo "    Node category is not set for CM subgraph nodes (known gap)."
