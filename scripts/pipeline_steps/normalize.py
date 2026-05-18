"""
Step 2 — normalize_nodes

Checks whether the two normalization JSON files already exist in norm_dir.
  - If both are present (and force=False): skips with a log message.
  - If either is missing: runs batch normalization against a NodeNorm endpoint.

Normalization is expensive and often pre-computed; the pipeline defaults to
skip-if-exists so reruns don't accidentally overwrite large JSON files.

Outputs (in norm_dir):
  semmed_normalized_full.json   — NodeNorm responses keyed by SemMed CUI
  ikraph_normalized_full.json   — NodeNorm responses keyed by iKraph external_id
"""
from __future__ import annotations

import csv
import gzip
import json
import sys
import time
from pathlib import Path
from typing import Dict, Any, List, Optional


def _batch_post(
    curies: List[str],
    endpoint: str,
    batch_size: int = 200,
) -> Dict[str, Any]:
    try:
        import requests  # type: ignore[import-untyped]
    except ImportError as exc:
        raise SystemExit("requests is required for normalization: pip install requests") from exc

    result: Dict[str, Any] = {}
    total = len(curies)
    for i in range(0, total, batch_size):
        batch = curies[i : i + batch_size]
        print(
            f"[normalize] batch {i // batch_size + 1}/{(total + batch_size - 1) // batch_size}"
            f" ({len(batch)} CURIEs)",
            file=sys.stderr,
        )
        try:
            resp = requests.post(
                endpoint,
                json={"curies": batch},
                headers={"Content-Type": "application/json"},
                timeout=60,
            )
            resp.raise_for_status()
            result.update(resp.json())
        except Exception as exc:  # noqa: BLE001
            print(f"WARNING: batch {i // batch_size + 1} failed: {exc}", file=sys.stderr)
            for c in batch:
                result[c] = None
        time.sleep(0.05)
    return result


def _collect_semmed_curies(predication_gz: Path) -> List[str]:
    """Collect unique SemMed CUIs (col 4 = subject, col 5 = object) from predication.csv.gz."""
    curies: set = set()
    with gzip.open(predication_gz, "rt", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if not row or row[0].strip().upper().startswith("PREDICATION_ID"):
                continue
            if len(row) >= 6:
                for col in (row[4].strip(), row[5].strip()):
                    if col:
                        curies.add(col)
    return sorted(curies)


def _collect_ikraph_curies(ner_json: Path) -> List[str]:
    """Collect unique iKraph external ids from NER_ID_dict_cap_final.json (streaming when ijson is available)."""
    try:
        import ijson  # type: ignore[import-untyped]

        curies: set[str] = set()
        with ner_json.open("rb") as fh:
            for entity in ijson.items(fh, "item"):
                ext_id = str(entity.get("id") or "").strip()
                if ext_id and ext_id != "NA":
                    curies.add(ext_id)
        return sorted(curies)
    except ImportError:
        print(
            "[normalize] WARNING: `ijson` not installed — loading full NER JSON for ID collection."
            "  pip install ijson",
            file=sys.stderr,
        )
        with ner_json.open("r", encoding="utf-8") as fh:
            entities = json.load(fh)
        curies: set[str] = set()
        for e in entities:
            ext_id = str(e.get("id") or "").strip()
            if ext_id and ext_id != "NA":
                curies.add(ext_id)
        return sorted(curies)


def normalize_nodes(
    predication_gz: Path,
    ner_json: Path,
    norm_dir: Path,
    endpoint: str = "http://127.0.0.1:8000/get_normalized_nodes",
    force: bool = False,
) -> tuple[Path, Path]:
    """
    Ensure semmed_normalized_full.json and ikraph_normalized_full.json exist in norm_dir.

    Skips normalization if both files already exist unless force=True.
    Returns (semmed_json_path, ikraph_json_path).
    """
    norm_dir.mkdir(parents=True, exist_ok=True)
    semmed_out = norm_dir / "semmed_normalized_full.json"
    ikraph_out = norm_dir / "ikraph_normalized_full.json"

    if semmed_out.exists() and ikraph_out.exists() and not force:
        print(
            "[normalize] Both normalization JSONs already exist — skipping."
            " Pass force=True to re-run.",
            file=sys.stderr,
        )
        return semmed_out, ikraph_out

    if not semmed_out.exists() or force:
        print("[normalize] Collecting SemMed CUIs...", file=sys.stderr)
        semmed_curies = _collect_semmed_curies(predication_gz)
        print(f"[normalize] {len(semmed_curies):,} unique SemMed CUIs", file=sys.stderr)
        semmed_norm = _batch_post(semmed_curies, endpoint)
        semmed_out.write_text(json.dumps(semmed_norm, ensure_ascii=False, indent=None))
        print(f"[normalize] wrote → {semmed_out}", file=sys.stderr)
    else:
        print(f"[normalize] {semmed_out.name} exists, skipping SemMed normalization.", file=sys.stderr)

    if not ikraph_out.exists() or force:
        print("[normalize] Collecting iKraph external IDs...", file=sys.stderr)
        ikraph_curies = _collect_ikraph_curies(ner_json)
        print(f"[normalize] {len(ikraph_curies):,} unique iKraph IDs", file=sys.stderr)
        ikraph_norm = _batch_post(ikraph_curies, endpoint)
        ikraph_out.write_text(json.dumps(ikraph_norm, ensure_ascii=False, indent=None))
        print(f"[normalize] wrote → {ikraph_out}", file=sys.stderr)
    else:
        print(f"[normalize] {ikraph_out.name} exists, skipping iKraph normalization.", file=sys.stderr)

    return semmed_out, ikraph_out
