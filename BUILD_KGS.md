# Building the General Med and CM Subgraph KGs

- **General Med KG** (full SemMed + iKraph, normalized) → `neo4j-general-med.combini.ncsa.illinois.edu`
- **CM Subgraph** (COMBO-matched seeds + 1-hop of General Med) → `neo4j-cm-subgraph.combini.ncsa.illinois.edu`

## Python pipeline (recommended — `pipeline-refactor` branch)

`scripts/build_pipeline.py` is the new single-entry-point pipeline that replaces
`build_graphs_from_raw.sh`.  Each stage is an independently testable Python function
in `scripts/pipeline_steps/`.  New features vs. the shell script:

- iKraph edge **direction** respected (direction=21 → subject/object swapped)
- iKraph **PMIDs** correctly joined after direction fix
- **`provided_by`** KGX field on every edge (infores CURIEs from source metadata)
- CM subgraph outputs renamed to `*_combo_seeds_and_1hop` / `*_combo_seeds_only`
- Normalization derived from JSON — no separate `semmed_nodes_normalized.csv` input needed
- Biolink predicates validated at startup

**Dependencies:** `pip install openpyxl ijson` (plus `requests` only if `--force-normalize`).
`ijson` is important for staying within RAM on full production runs—it streams DBRelations /
PubMedList / NER / iKraph norm JSON row-by-row. Without it you get a WARNING and Python may
resident-load multi‑GB files.

```bash
SEMMED=/Users/drshika2/neo4jexploration/semmed_data
IKRAPH=/Users/drshika2/neo4jexploration/iKraph_raw/iKraph_full
NORM=semmed_ikraph_normalized

python3 scripts/build_pipeline.py \
  --semmed-dir        $SEMMED \
  --ikraph-dir        $IKRAPH \
  --reltype-xlsx      ikraph_reltype_to_biolink_mapping_review.xlsx \
  --combo-owl         /Users/drshika2/Downloads/COMBO_20260115.owl \
  --norm-dir          $NORM \
  --output-dir        /tmp/kg_build_out
```

Skip one graph:
```bash
python3 scripts/build_pipeline.py ... --skip-cm-subgraph   # gen-med only
python3 scripts/build_pipeline.py ... --skip-gen-med        # CM subgraph only
```

Re-run normalization (expensive — only if NodeNorm JSON files need refreshing):
```bash
python3 scripts/build_pipeline.py ... --force-normalize --nodenorm-endpoint http://...
```

Run tests:
```bash
pytest tests/test_pipeline.py -v
```

---

## Legacy shell pipeline

### Step 0: Generate the iKraph reltype → Biolink mapping

Run once (or any time the spreadsheet is updated):

```bash
python3 scripts/generate_ikraph_reltype_map.py \
  --xlsx         ikraph_reltype_to_biolink_mapping_review.xlsx \
  --reltypeint-json /Users/drshika2/neo4jexploration/iKraph_raw/iKraph_full/RelTypeInt.json \
  --output       semmed_ikraph_normalized/ikraph_reltype_map.csv
```

This reads `ikraph_reltype_to_biolink_mapping_review.xlsx` (Halil's column preferred, Cesar's as fallback) and writes `semmed_ikraph_normalized/ikraph_reltype_map.csv`. To change any mapping, edit the spreadsheet and re-run — no Python code changes needed.

---

### One Command (both KGs, legacy shell)

Run from `NodeNormalization/` root:

```bash
SEMMED=/Users/drshika2/neo4jexploration/semmed_data
IKRAPH=/Users/drshika2/neo4jexploration/iKraph_raw
NORM=semmed_ikraph_normalized

./scripts/build_graphs_from_raw.sh \
  --semmed-nodes              $NORM/neo4j_import_ready_normalized/semmed_nodes_normalized.csv \
  --semmed-predication        $SEMMED/predication.csv.gz \
  --semmed-concepts-csv       $SEMMED/concept.csv \
  --ikraph-nodes-json         $IKRAPH/iKraph_full/NER_ID_dict_cap_final.json \
  --ikraph-db-json            $IKRAPH/iKraph_full/DBRelations.json \
  --ikraph-pubmed-json        $IKRAPH/iKraph_full/PubMedList.json \
  --ikraph-normalized-json    $NORM/ikraph_normalized_full.with_newly_normalized_20260123_v6_20260218.json \
  --ikraph-reltype-map        $NORM/ikraph_reltype_map.csv \
  --ikraph-edges-cleaned      $NORM/ikraph_edges_cleaned.csv \
  --semmed-normalized-json    $NORM/semmed_normalized_full_with_pubchem.json \
  --combo-owl                 /Users/drshika2/Downloads/COMBO_20260115.owl \
  --output-dir                /tmp/kg_build_out
```


Links to the file in box: 
https://uofi.box.com/s/8sjd7r167w0miiahwdxjma8dcick76me
- semmed_nodes_normalized.csv
- connections.csv.gz
- concept.csv
- predication.csv.gz
- ikraph_edges_cleaned.csv
- ikraph_normalized_full.with_newly_normalized_20260123_v6_20260218.json
- semmed_normalized_full_with_pubchem.json


Add `--stream-ikraph-json` if you're tight on RAM (requires `ijson`).

Outputs:
- `/tmp/kg_build_out/gen_med_graph_neo4j/` — General Med KG, Neo4j-import-ready
- `/tmp/kg_build_out/cm_subgraph/` — CM subgraph TSVs

---

## Load into K8s

### General Med KG

```bash
cd /Users/drshika2/neo4jexploration/combini-kubernetes/neo4j

KUBECONFIG_PATH="/Users/drshika2/neo4jexploration/combini-kubernetes/combini.yaml" \
NAMESPACE="neo4j" \
RELEASE_NAME="my-neo4j-semmed-ikraph-release" \
NEO4J_VERSION="2025.03.0" \
USE_NORMALIZED=1 \
NORMALIZED_DIR="/tmp/kg_build_out/gen_med_graph_neo4j" \
./load-semmed-ikraph-to-new-instance.sh \
  /Users/drshika2/neo4jexploration/semmed_data \
  /Users/drshika2/NodeNormalization/semmed_ikraph_normalized/ikraph_edges_cleaned.csv
```

Endpoint: `https://neo4j-general-med.combini.ncsa.illinois.edu/browser/`

### CM Subgraph

```bash
cd /Users/drshika2/neo4jexploration/combini-kubernetes/neo4j

./load-cm-subgraph-to-k8s.sh \
  /Users/drshika2/NodeNormalization \
  /Users/drshika2/NodeNormalization/.env
```

Skip the rebuild and just reload data:

```bash
CLEAR_EXISTING=0 ./load-cm-subgraph-to-k8s.sh
```

Endpoint: `https://neo4j-cm-subgraph.combini.ncsa.illinois.edu/browser/`

---

## Validate

```bash
source .env

# General Med KG
cypher-shell -a "$GENERAL_MED_URI" -u "$GENERAL_MED_USERNAME" -p "$GENERAL_MED_PASSWORD" \
  --non-interactive "MATCH (n) RETURN count(n) AS nodes;"

# CM Subgraph
cypher-shell -a "$CM_SUBGRAPH_URI" -u "$CM_SUBGRAPH_USERNAME" -p "$CM_SUBGRAPH_PASSWORD" \
  --non-interactive "MATCH (n) RETURN count(n) AS nodes;"
```

Or run the verify script against both at once:

```bash
python3 scripts/verify_graph_match.py \
  --gen-med-dir     /tmp/kg_build_out/gen_med_graph_neo4j \
  --cm-subgraph-dir /tmp/kg_build_out/cm_subgraph \
  --env-file        .env
```

---

## Build Only One KG

Skip General Med KG:
```bash
--skip-gen-med
```

Skip CM subgraph:
```bash
--skip-cm-subgraph
```

Or for the CM subgraph only (quick path, hardcoded paths):
```bash
./scripts/run_cm_subgraph_pipeline.sh
```

Intervention-only extraction:
```bash
EXTRACT_ARGS='--focus-intervention-umls-only' ./scripts/run_cm_subgraph_pipeline.sh
```

---

## KGX Notes

- **General Med KG**: all required edge fields (`subject`, `object`, `predicate`, `knowledge_level`, `agent_type`) are baked in — no post-import patching needed.
- **CM subgraph**: run `patch_kgx_fields.py --graph cm-subgraph` after loading to backfill `SEMMED_EDGE` fields. `IKRAPH_EDGE.predicate` and node `category` are known gaps in that pipeline.
