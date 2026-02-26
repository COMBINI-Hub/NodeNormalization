# COMBO Retention Runbook

This runbook executes the checklist for fixing zero retained SemMedDB/iKraph edges.

## One-command iteration

```bash
python3 scripts/run_combo_retention_iteration.py \
  --combo-rtf "COMBO_20251022.rtf" \
  --crosswalk-files path/to/crosswalk1.tsv path/to/crosswalk2.json \
  --semmed-edge-file semmeddb_edges_cleaned.csv \
  --ikraph-edge-file ikraph_edges_cleaned.csv \
  --ikraph-node-dict path/to/NER_ID_dict_cap_final.json \
  --output-dir cam_subgraph_output_combo
```

## Outputs produced

- `combo_concepts_raw.tsv`
- `combo_seed_mappings.tsv`
- `combo_bridge_table.tsv`
- `combo_bridge_crosswalk.tsv`
- `combo_endpoint_candidates.tsv`
- `combo_mrconso_exact_mappings.tsv`
- `combo_mrconso_exact_review.tsv`
- `combo_unresolved_after_mrconso.tsv`
- `combo_mrconso_review_promoted.tsv`
- `combo_unresolved_after_mrconso_review.tsv`
- `combo_variant_parent_mappings.tsv`
- `combo_unresolved_after_variant_parent.tsv`
- `combo_cam_lexicon_mappings.tsv`
- `combo_unresolved_after_cam_lexicon.tsv`
- `combo_manual_curation.tsv`
- `combo_manual_curated_mappings.tsv`
- `combo_unresolved_after_manual_curation.tsv`
- `combo_modifier_stripping_mappings.tsv`
- `combo_unresolved_after_modifier.tsv`
- `combo_cam_lexicon_mappings.expanded.tsv`
- `combo_unresolved_after_cam_lexicon.expanded.tsv`
- `combo_hierarchy_backoff_mappings.tsv`
- `combo_unresolved_after_hierarchy_backoff.tsv`
- `combo_semantic_mappings.tsv`
- `combo_seed_mappings.with_semantic.tsv`
- `combo_unresolved_after_semantic.tsv`
- `cam_subgraph_output_combo/cam_nodes.tsv`
- `cam_subgraph_output_combo/cam_edges.tsv`
- `cam_subgraph_output_combo/cam_stats.json`
- `combo_mapping_qc.json`
- `COMBO_SUBGRAPH_RUN_REPORT.md`

## New helper scripts

- `scripts/build_endpoint_candidate_dictionary.py`
  - Scans SemMedDB/iKraph endpoints and emits semantic candidate dictionary rows:
    - `preferred_identifier`
    - `preferred_label`
    - `evidence`
    - `source_file`
- `scripts/build_combo_bridge_table.py`
  - Expands COMBO IDs into equivalence clusters using mapping files.
  - Emits:
    - detailed bridge table: `combo_bridge_table.tsv`
    - crosswalk format: `combo_bridge_crosswalk.tsv`
- `scripts/run_combo_retention_iteration.py`
  - Runs full iteration in sequence, including MRCONSO exact-match pass when `MRCONSO.RRF` exists.
- `scripts/apply_mrconso_exact_mappings.py`
  - Applies conservative exact MRCONSO matches to unresolved concepts.
  - Emits accepted mappings, review bucket, and remaining unresolved.
- `scripts/promote_mrconso_review_rules.py`
  - Promotes a safe subset of MRCONSO review rows (conservative MeSH label matches).
  - Leaves risky rows in review and updates unresolved carry-forward file.
- `scripts/promote_variant_to_parent_mappings.py`
  - Maps unresolved variants to already-mapped parent concepts (non-exact).
- `scripts/apply_cam_lexicon_mappings.py`
  - Applies curated CAM lexicon variant mappings (non-exact).
- `scripts/prepare_manual_curation_from_review.py`
  - Prepares a manual curation template from remaining review rows.
- `scripts/apply_manual_curation_mappings.py`
  - Applies manual curation selections to unresolved rows.
- `scripts/promote_modifier_stripping_mappings.py`
  - Strips lexical modifiers and maps to already-mapped core concepts (non-exact).
- `scripts/promote_hierarchy_backoff_mappings.py`
  - Maps unresolved concepts to nearest mapped COMBO ancestor (non-exact).

## Notes

- If `iKraph` endpoints are biokdeid integers, provide `--ikraph-node-dict`.
- If labels are sparse in your normalized files, endpoint candidates can still be emitted with `--endpoint-fallback-label-to-id`.
- You can add curated dictionaries through `--extra-candidate-files` in the orchestrator.
- You can tune runtime/recall with:
  - `--endpoint-max-unique-endpoints`
  - `--semantic-max-candidates`
- Non-exact mappings are explicitly tagged using:
  - `method` (e.g. `variant_parent_mapping`, `cam_lexicon_mapping`, `manual_curation`)
  - `mapping_relation` (e.g. `close_match`, `narrow_to_parent`, `manual_curated`)
