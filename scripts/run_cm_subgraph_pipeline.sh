#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/drshika2/NodeNormalization"
OUTPUT_DIR="$ROOT_DIR/semmed_ikraph_normalized/combo_subgraph"
ENV_FILE="$ROOT_DIR/.env"

# Optional:
#   EXTRACT_ARGS='--focus-intervention-umls-only' ./scripts/run_cm_subgraph_pipeline.sh
#   CLEAR_EXISTING=0 ./scripts/run_cm_subgraph_pipeline.sh
EXTRACT_ARGS="${EXTRACT_ARGS:-}"
CLEAR_EXISTING="${CLEAR_EXISTING:-1}"

echo "[1/3] Extracting COMBO-matched seeds + 1-hop edges"
python3 "$ROOT_DIR/extract_combo_semmed_ikraph_subgraph.py" $EXTRACT_ARGS

echo "[2/3] Building SemMed concept/type table for extracted edges"
python3 "$ROOT_DIR/build_semmed_concepts_from_normalized.py" \
  --semmed-edges-file "$OUTPUT_DIR/semmed_edges_seed_adjacent.tsv" \
  --normalized-json-file "$ROOT_DIR/semmed_ikraph_normalized/semmed_normalized_full_with_pubchem.json" \
  --semmed-concepts-csv "/Users/drshika2/neo4jexploration/semmed_data/concept.csv" \
  --output-file "$OUTPUT_DIR/semmed_concepts_seed_adjacent.tsv"

echo "[3/3] Loading into Neo4j Aura"
if [[ "$CLEAR_EXISTING" == "1" ]]; then
  python3 "$ROOT_DIR/load_combo_subgraph_to_aura.py" \
    --env-file "$ENV_FILE" \
    --clear-existing
else
  python3 "$ROOT_DIR/load_combo_subgraph_to_aura.py" \
    --env-file "$ENV_FILE"
fi

echo "CM subgraph pipeline complete."
