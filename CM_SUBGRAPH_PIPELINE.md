# CM Subgraph Pipeline (Current)

This is the current, reproducible path from raw inputs to the Neo4j CM subgraph.

## One Command

```bash
./scripts/run_cm_subgraph_pipeline.sh
```

By default this:
- extracts all COMBO-matched SemMed/iKraph nodes plus 1-hop neighbors
- uses exact-only mappings (no embedding-based mapping inclusion)
- rebuilds `semmed_concepts_seed_adjacent.tsv`
- clears Neo4j and loads the fresh graph

## Inputs

- `COMBO_20260115_with_skos.owl`
- `semmed_ikraph_normalized/semmed_normalized_full_with_pubchem.json`
- `semmed_ikraph_normalized/ikraph_normalized_full.with_newly_normalized_20260123_v6_20260218.json`
- `/Users/drshika2/neo4jexploration/semmed_data/concept.csv`
- `/Users/drshika2/neo4jexploration/semmed_data/connections.csv`
- `semmed_ikraph_normalized/ikraph_edges_cleaned.csv`
- `.env` (Neo4j creds)

## Main Outputs

- `semmed_ikraph_normalized/combo_subgraph/combo_seed_nodes.tsv`
- `semmed_ikraph_normalized/combo_subgraph/semmed_edges_seed_adjacent.tsv`
- `semmed_ikraph_normalized/combo_subgraph/ikraph_edges_seed_adjacent.tsv`
- `semmed_ikraph_normalized/combo_subgraph/semmed_concepts_seed_adjacent.tsv`
- `semmed_ikraph_normalized/combo_subgraph/combo_subgraph_summary.json`

## Useful Variants

Run without clearing Neo4j:

```bash
CLEAR_EXISTING=0 ./scripts/run_cm_subgraph_pipeline.sh
```

Intervention-only extraction:

```bash
EXTRACT_ARGS='--focus-intervention-umls-only' ./scripts/run_cm_subgraph_pipeline.sh
```
