"""
Step 5 — extract_cm_subgraph

Calls the existing extract_combo_semmed_ikraph_subgraph.py script via subprocess,
then renames its outputs to the new naming convention:

  combo_seed_nodes.tsv               (unchanged)
  combo_normalization_links.tsv      (unchanged)
  semmed_edges_seed_adjacent.tsv  →  semmed_edges_combo_seeds_and_1hop.tsv
  ikraph_edges_seed_adjacent.tsv  →  ikraph_edges_combo_seeds_and_1hop.tsv
  semmed_edges_seed_induced.tsv   →  semmed_edges_combo_seeds_only.tsv
  ikraph_edges_seed_induced.tsv   →  ikraph_edges_combo_seeds_only.tsv

The "seeds_and_1hop" files contain edges where at least one endpoint is a
COMBO-matched seed node (seed + all direct 1-hop neighbors in the KG).
The "seeds_only" files contain edges where both endpoints are COMBO seeds
(induced subgraph of the seed set).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional


# Map from the script's native output names to the new pipeline names.
_RENAME_MAP = {
    "semmed_edges_seed_adjacent.tsv": "semmed_edges_combo_seeds_and_1hop.tsv",
    "ikraph_edges_seed_adjacent.tsv": "ikraph_edges_combo_seeds_and_1hop.tsv",
    "semmed_edges_seed_induced.tsv": "semmed_edges_combo_seeds_only.tsv",
    "ikraph_edges_seed_induced.tsv": "ikraph_edges_combo_seeds_only.tsv",
}


def extract_cm_subgraph(
    semmed_normalized_json: Path,
    ikraph_normalized_json: Path,
    semmed_concepts_csv: Path,
    ikraph_edges_cleaned: Path,
    combo_owl: Path,
    output_dir: Path,
    *,
    ikraph_nodes_json: Optional[Path] = None,
    semmed_connections_csv: Optional[Path] = None,
    embedding_best: Optional[Path] = None,
    ikraph_names_flat: Optional[Path] = None,
    denylist_file: Optional[Path] = None,
    extra_args: Optional[List[str]] = None,
    script_path: Optional[Path] = None,
) -> List[Path]:
    """
    Run the CM subgraph extraction and rename outputs to the new naming convention.

    Returns the list of output file paths (after renaming).
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if script_path is None:
        # Locate extract_combo_semmed_ikraph_subgraph.py relative to this file:
        # scripts/pipeline_steps/ → scripts/ → repo_root/
        script_path = Path(__file__).parent.parent.parent / "extract_combo_semmed_ikraph_subgraph.py"
    if not script_path.exists():
        raise FileNotFoundError(f"Extraction script not found: {script_path}")

    cmd = [
        sys.executable,
        str(script_path),
        "--semmed-normalized", str(semmed_normalized_json),
        "--ikraph-normalized", str(ikraph_normalized_json),
        "--semmed-concepts", str(semmed_concepts_csv),
        "--ikraph-edges", str(ikraph_edges_cleaned),
        "--combo-owl", str(combo_owl),
        "--output-dir", str(output_dir),
    ]
    if ikraph_nodes_json:
        cmd += ["--ikraph-nodes", str(ikraph_nodes_json)]
    if semmed_connections_csv:
        cmd += ["--semmed-edges", str(semmed_connections_csv)]
    if embedding_best and embedding_best.exists():
        cmd += ["--embedding-best", str(embedding_best)]
    if ikraph_names_flat and ikraph_names_flat.exists():
        cmd += ["--ikraph-names-flat", str(ikraph_names_flat)]
    if denylist_file and denylist_file.exists():
        cmd += ["--denylist-file", str(denylist_file)]
    if extra_args:
        cmd.extend(extra_args)

    print(f"[extract_cm] running: {' '.join(cmd[:6])} ...", file=sys.stderr)
    result = subprocess.run(cmd, check=True)
    if result.returncode != 0:
        raise RuntimeError(f"Extraction script exited with code {result.returncode}")

    # Rename outputs to the new naming convention
    outputs: List[Path] = []
    for old_name, new_name in _RENAME_MAP.items():
        old_path = output_dir / old_name
        new_path = output_dir / new_name
        if old_path.exists():
            shutil.move(str(old_path), str(new_path))
            print(f"[extract_cm] renamed {old_name} → {new_name}", file=sys.stderr)
            outputs.append(new_path)
        elif new_path.exists():
            outputs.append(new_path)

    # Also collect unchanged outputs
    for name in (
        "combo_seed_nodes.tsv",
        "combo_normalization_links.tsv",
        "combo_subgraph_summary.json",
    ):
        p = output_dir / name
        if p.exists():
            outputs.append(p)

    return outputs
