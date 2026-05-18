#!/usr/bin/env python3
"""
Regenerate Biolink predicate allowlist bundled with scripts/pipeline_steps/data/.

The list mirrors the Biolink Model as loaded by Biolink Model Toolkit (bmt), i.e.
all formatted CURIEs under the ``related_to`` slot hierarchy (Mixin tree included).
This matches predicates documented in BiOLink Model for Translator-style KG edges.

Requires: pip install bmt requests pyyaml (same as repo node_normalizer stack).

Usage:
    ./venv/bin/python scripts/tools/refresh_biolink_predicate_allowlist.py
"""
from __future__ import annotations

from pathlib import Path

from bmt import Toolkit


def main() -> None:
    toolkit = Toolkit()
    predicates = sorted(toolkit.get_descendants("related to", formatted=True, mixin=True))
    curated = [p for p in predicates if p.startswith("biolink:")]

    repo_root = Path(__file__).resolve().parents[2]
    out_path = (
        repo_root
        / "scripts"
        / "pipeline_steps"
        / "data"
        / "biolink_model_predicates_bmt-default.txt"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    meta = toolkit.get_model_version()
    header = (
        "# Biolink predicates = descendants of BiOLink Model slot \"related_to\" (Mixin tree).\n"
        "# Source: Biolink Model Toolkit (bmt) default schema + predicate mappings.\n"
        f'# Model version metadata from Toolkit.get_model_version(): {meta!r}\n'
        "# Regenerate: python scripts/tools/refresh_biolink_predicate_allowlist.py\n"
    )
    out_path.write_text(header + "\n".join(curated) + "\n", encoding="utf-8")
    print(f"Wrote {len(curated)} predicates → {out_path}")


if __name__ == "__main__":
    main()
