#!/usr/bin/env python3
"""
CAM subgraph extractor with ontology-rooted scope control.

Pipeline:
1. Seed from authoritative roots (FoodOn/TCDO/LSFO/selected ChEBI roles/NCBI Taxon).
2. Expand by closure using subClassOf + approved part_of/has_role relations.
3. Prune obsolete/deprecated terms.
4. Map to canonical identifiers (NodeNorm/UMLS-compatible tables optional).
5. Emit CAM nodes/edges/statistics for downstream graph extraction.
"""

from __future__ import annotations

import argparse
import csv
import importlib
import json
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from rdflib import BNode, Graph, Literal, Namespace, OWL, RDF, RDFS, URIRef


OBO_BASE = "http://purl.obolibrary.org/obo/"
# TCDO uses OntoTCM base, not OBO (see ontologies/tcdo.owl)
TCDO_BASE = "http://OntoTCM.org.cn/ontologies/"
OIO = Namespace("http://www.geneontology.org/formats/oboInOwl#")

REL_PART_OF = URIRef(f"{OBO_BASE}BFO_0000050")
REL_HAS_ROLE = URIRef(f"{OBO_BASE}RO_0000087")
REL_PART_OF_ALT = URIRef(f"{OBO_BASE}RO_0000052")
APPROVED_RELATIONS: Set[URIRef] = {RDFS.subClassOf, REL_PART_OF, REL_HAS_ROLE, REL_PART_OF_ALT}

ONTOLOGY_FILENAMES = {
    "foodon": "foodon.owl",
    "tcdo": "tcdo.owl",
    "lsfo": "lsfo.owl",
    "chebi": "chebi.owl",
    "ncbitaxon": "ncbitaxon.obo",
}

DEFAULT_CAM_ROOTS: Dict[str, List[str]] = {
    "foodon": ["FOODON:00001002"],
    "tcdo": ["TCDO:0000001"],  # TCDO:0000000 does not exist in ontology; 0000001 is root thing
    "lsfo": ["LFID:0000000"],
    "chebi": ["CHEBI:50906"],  # role
    "ncbitaxon": ["NCBITaxon:1"],
}

PREFIX_TO_BIOLINK = {
    "FOODON": "biolink:Food",
    "CHEBI": "biolink:ChemicalEntity",
    "NCBITaxon": "biolink:OrganismTaxon",
    "UMLS": "biolink:NamedThing",
    "MESH": "biolink:NamedThing",
    "PUBCHEM.COMPOUND": "biolink:SmallMolecule",
    "DRUGBANK": "biolink:Drug",
}


@dataclass
class MappingRecord:
    canonical_id: str
    category: Optional[str]
    confidence: Optional[float]
    source_file: str
    label: Optional[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract CAM ontology subgraph with canonical mapping.")
    parser.add_argument(
        "--ontology-dir",
        default="ontologies",
        help="Directory containing ontology files (foodon.owl, tcdo.owl, lsfo.owl, chebi.owl, ncbitaxon.obo).",
    )
    parser.add_argument(
        "--roots-json",
        default=None,
        help='Optional JSON file with explicit CAM roots, e.g. {"foodon": [...], "chebi": [...]}',
    )
    parser.add_argument(
        "--normalized-files",
        nargs="*",
        default=[],
        help="Optional normalized mapping files (JSON/JSONL/CSV/TSV) for canonical ID mapping.",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=None,
        help="Optional confidence threshold for normalized mappings (0.0-1.0).",
    )
    parser.add_argument(
        "--require-confidence",
        action="store_true",
        help="If set, drop normalized mappings lacking usable confidence metadata.",
    )
    parser.add_argument(
        "--output-dir",
        default="cam_subgraph_output",
        help="Output directory for CAM node/edge/stat files.",
    )
    parser.add_argument(
        "--semmed-edge-file",
        default=None,
        help="Optional SemMedDB edge CSV (e.g., semmeddb_relationships_neo4j.csv) to filter into CAM.",
    )
    parser.add_argument(
        "--semmed-min-frequency",
        type=int,
        default=1,
        help="Minimum SemMedDB edge frequency to keep when --semmed-edge-file is provided.",
    )
    parser.add_argument(
        "--ikraph-edge-file",
        default=None,
        help="Optional iKraph edge CSV (e.g., ikraph_edges_cleaned.csv) to filter into CAM.",
    )
    parser.add_argument(
        "--ikraph-node-dict",
        default=None,
        help="Optional iKraph node list JSON (NER_ID_dict_cap_final.json) to map biokdeid -> CURIE for edge endpoints.",
    )
    parser.add_argument(
        "--analysis-doc",
        default="CAM_ANALYSIS.md",
        help="Markdown path for CAM/SemMed/iKraph analysis (default: CAM_ANALYSIS.md in output dir). Set to empty to disable.",
    )
    parser.add_argument(
        "--no-analysis-doc",
        action="store_true",
        help="Do not write the analysis markdown file.",
    )
    parser.add_argument(
        "--overlap-relaxed-summary",
        default=None,
        help="Optional JSON summary for relaxed SemMed/iKraph overlap (for analysis doc).",
    )
    parser.add_argument(
        "--semmed-summary",
        default=None,
        help="Optional JSON summary for SemMedDB relationship build (for analysis doc).",
    )
    return parser.parse_args()


def normalize_curie(raw: str) -> str:
    return (raw or "").strip().replace(" ", "")


def curie_to_iri(curie: str) -> Optional[str]:
    curie = normalize_curie(curie)
    if not curie or ":" not in curie:
        return None
    prefix, local = curie.split(":", 1)
    if prefix == "TCDO":
        return f"{TCDO_BASE}{prefix}_{local}"
    if prefix in {"FOODON", "LSFO", "CHEBI", "NCBITaxon"}:
        return f"{OBO_BASE}{prefix}_{local}"
    return None


def iri_to_curie(iri: str) -> str:
    if iri.startswith(TCDO_BASE):
        tail = iri[len(TCDO_BASE) :]
        if "_" in tail:
            prefix, local = tail.split("_", 1)
            return f"{prefix}:{local}"
    if iri.startswith(OBO_BASE):
        tail = iri[len(OBO_BASE) :]
        if "_" in tail:
            prefix, local = tail.split("_", 1)
            return f"{prefix}:{local}"
    return iri


def normalize_predicate(pred_iri: URIRef) -> str:
    if pred_iri == RDFS.subClassOf:
        return "rdfs:subClassOf"
    if pred_iri in {REL_PART_OF, REL_PART_OF_ALT}:
        return "BFO:0000050_part_of"
    if pred_iri == REL_HAS_ROLE:
        return "RO:0000087_has_role"
    return str(pred_iri)


def parse_bool_literal(value: Any) -> bool:
    if isinstance(value, Literal):
        text = str(value).strip().lower()
    else:
        text = str(value).strip().lower()
    return text in {"1", "true", "yes"}


def parse_confidence(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        val = float(value)
        if val > 1.0 and val <= 5.0:
            return val / 5.0
        return max(0.0, min(1.0, val))

    text = str(value).strip().lower()
    if not text:
        return None
    if text in {"high", "high-confidence"}:
        return 0.9
    if text in {"medium", "mid", "moderate"}:
        return 0.6
    if text in {"low", "low-confidence"}:
        return 0.3
    if "star" in text:
        for ch in text:
            if ch.isdigit():
                return max(0.0, min(1.0, float(ch) / 5.0))
    try:
        num = float(text)
        if num > 1.0 and num <= 5.0:
            return num / 5.0
        return max(0.0, min(1.0, num))
    except ValueError:
        return None


def pick_first(record: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
    for key in keys:
        if key in record and record[key] not in (None, "", []):
            return record[key]
    return None


def infer_biolink_from_id(identifier: str) -> str:
    prefix = identifier.split(":", 1)[0] if ":" in identifier else identifier
    return PREFIX_TO_BIOLINK.get(prefix, "biolink:NamedThing")


def canonicalize_id(identifier: str, mapping_index: Dict[str, MappingRecord]) -> str:
    normalized = normalize_curie(identifier)
    if not normalized:
        return normalized
    mapping = mapping_index.get(normalized)
    if mapping:
        return mapping.canonical_id
    return normalized


def choose_better_mapping(current: Optional[MappingRecord], candidate: MappingRecord) -> MappingRecord:
    if current is None:
        return candidate
    cur_conf = current.confidence if current.confidence is not None else -1.0
    cand_conf = candidate.confidence if candidate.confidence is not None else -1.0
    if cand_conf > cur_conf:
        return candidate
    if cand_conf == cur_conf and len(candidate.canonical_id) < len(current.canonical_id):
        return candidate
    return current


def load_roots(path: Optional[str]) -> Dict[str, List[str]]:
    roots = {k: list(v) for k, v in DEFAULT_CAM_ROOTS.items()}
    if not path:
        return roots
    with open(path, "r", encoding="utf-8") as handle:
        user_roots = json.load(handle)
    for source, values in user_roots.items():
        if isinstance(values, list):
            roots[source] = [normalize_curie(v) for v in values if v]
    return roots


def load_mapping_index(
    paths: List[str],
    min_confidence: Optional[float],
    require_confidence: bool,
) -> Dict[str, MappingRecord]:
    index: Dict[str, MappingRecord] = {}
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            print(f"[warn] Mapping file missing, skipping: {path}")
            continue
        lower = path.name.lower()
        if lower.endswith(".json"):
            consume_json_mapping(path, index, min_confidence, require_confidence)
        elif lower.endswith(".jsonl"):
            consume_jsonl_mapping(path, index, min_confidence, require_confidence)
        elif lower.endswith(".csv") or lower.endswith(".tsv"):
            consume_table_mapping(path, index, min_confidence, require_confidence)
        else:
            print(f"[warn] Unsupported mapping format, skipping: {path}")
    return index


def apply_mapping_row(
    input_ids: List[str],
    canonical_id: str,
    category: Optional[str],
    confidence_raw: Any,
    label: Optional[str],
    source_file: str,
    index: Dict[str, MappingRecord],
    min_confidence: Optional[float],
    require_confidence: bool,
) -> None:
    canonical_id = normalize_curie(canonical_id)
    if not canonical_id:
        return
    confidence = parse_confidence(confidence_raw)
    if require_confidence and confidence is None:
        return
    if min_confidence is not None and confidence is not None and confidence < min_confidence:
        return
    if min_confidence is not None and confidence is None and require_confidence:
        return

    rec = MappingRecord(
        canonical_id=canonical_id,
        category=category,
        confidence=confidence,
        source_file=source_file,
        label=label,
    )
    for raw in input_ids:
        curie = normalize_curie(raw)
        if not curie:
            continue
        index[curie] = choose_better_mapping(index.get(curie), rec)


def consume_json_mapping(
    path: Path,
    index: Dict[str, MappingRecord],
    min_confidence: Optional[float],
    require_confidence: bool,
) -> None:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if isinstance(payload, dict):
        # NodeNorm-like: {input_id: {id: {identifier}, equivalent_identifiers: [...], type: [...]}}
        for input_id, obj in payload.items():
            if not isinstance(obj, dict):
                continue
            canonical = (((obj.get("id") or {}).get("identifier")) if obj.get("id") else None) or input_id
            label = ((obj.get("id") or {}).get("label")) if obj.get("id") else None
            types = obj.get("type")
            if isinstance(types, list):
                category = types[0] if types else None
            else:
                category = types
            confidence = pick_first(obj, ["confidence_score", "confidence", "score"])

            input_ids = [input_id, canonical]
            for eq in obj.get("equivalent_identifiers", []) or []:
                if isinstance(eq, dict):
                    identifier = eq.get("identifier")
                    if identifier:
                        input_ids.append(identifier)

            apply_mapping_row(
                input_ids=input_ids,
                canonical_id=canonical,
                category=category,
                confidence_raw=confidence,
                label=label,
                source_file=str(path),
                index=index,
                min_confidence=min_confidence,
                require_confidence=require_confidence,
            )
        return

    if isinstance(payload, list):
        for row in payload:
            if not isinstance(row, dict):
                continue
            input_id = pick_first(row, ["input_curie", "input_id", "identifier", "id", "curie"])
            canonical = pick_first(row, ["preferred_identifier", "canonical_id", "normalized_identifier", "identifier", "id"])
            category = pick_first(row, ["type", "types", "category", "biolink_type"])
            confidence = pick_first(row, ["confidence", "confidence_score", "score"])
            label = pick_first(row, ["preferred_label", "label", "name"])
            if isinstance(category, list):
                category = category[0] if category else None
            if isinstance(category, str) and ";" in category:
                category = category.split(";", 1)[0]
            if input_id and canonical:
                apply_mapping_row(
                    input_ids=[str(input_id)],
                    canonical_id=str(canonical),
                    category=str(category) if category else None,
                    confidence_raw=confidence,
                    label=str(label) if label else None,
                    source_file=str(path),
                    index=index,
                    min_confidence=min_confidence,
                    require_confidence=require_confidence,
                )


def consume_jsonl_mapping(
    path: Path,
    index: Dict[str, MappingRecord],
    min_confidence: Optional[float],
    require_confidence: bool,
) -> None:
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            input_id = pick_first(row, ["input_curie", "input_id", "identifier", "id", "curie"])
            canonical = pick_first(row, ["preferred_identifier", "canonical_id", "normalized_identifier", "identifier", "id"])
            category = pick_first(row, ["type", "types", "category", "biolink_type"])
            confidence = pick_first(row, ["confidence", "confidence_score", "score"])
            label = pick_first(row, ["preferred_label", "label", "name"])
            if isinstance(category, list):
                category = category[0] if category else None
            if input_id and canonical:
                apply_mapping_row(
                    input_ids=[str(input_id)],
                    canonical_id=str(canonical),
                    category=str(category) if category else None,
                    confidence_raw=confidence,
                    label=str(label) if label else None,
                    source_file=str(path),
                    index=index,
                    min_confidence=min_confidence,
                    require_confidence=require_confidence,
                )


def consume_table_mapping(
    path: Path,
    index: Dict[str, MappingRecord],
    min_confidence: Optional[float],
    require_confidence: bool,
) -> None:
    delimiter = "\t" if path.suffix.lower() == ".tsv" else ","
    with open(path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        for row in reader:
            input_id = pick_first(row, ["input_curie", "input_id", "identifier", "id", "curie", "umls_key"])
            canonical = pick_first(
                row,
                ["preferred_identifier", "canonical_id", "normalized_identifier", "identifier", "id", "pubchem_curie"],
            )
            category = pick_first(row, ["type", "types", "category", "biolink_type"])
            confidence = pick_first(row, ["confidence", "confidence_score", "score"])
            label = pick_first(row, ["preferred_label", "label", "name", "pubchem_name"])
            if isinstance(category, str) and ";" in category:
                category = category.split(";", 1)[0]
            if input_id and canonical:
                apply_mapping_row(
                    input_ids=[str(input_id)],
                    canonical_id=str(canonical),
                    category=str(category) if category else None,
                    confidence_raw=confidence,
                    label=str(label) if label else None,
                    source_file=str(path),
                    index=index,
                    min_confidence=min_confidence,
                    require_confidence=require_confidence,
                )


def build_owl_index(graph: Graph) -> Tuple[Set[str], Dict[str, str], Dict[str, str]]:
    labels: Dict[str, str] = {}
    classes: Set[str] = set()
    id_to_iri: Dict[str, str] = {}
    for subj in graph.subjects(RDF.type, OWL.Class):
        if isinstance(subj, URIRef):
            classes.add(str(subj))
    for subj, _, obj in graph.triples((None, RDFS.label, None)):
        if isinstance(subj, URIRef) and isinstance(obj, Literal):
            labels[str(subj)] = str(obj)
    for subj, _, obj in graph.triples((None, OIO.id, None)):
        if isinstance(subj, URIRef) and isinstance(obj, Literal):
            id_to_iri[str(obj)] = str(subj)
    return classes, labels, id_to_iri


def extract_relation_edges(graph: Graph, classes: Set[str]) -> List[Tuple[str, str, URIRef]]:
    edges: List[Tuple[str, str, URIRef]] = []

    # Direct class-to-class edges.
    for child, pred, parent in graph.triples((None, None, None)):
        if pred not in APPROVED_RELATIONS:
            continue
        if isinstance(child, URIRef) and isinstance(parent, URIRef):
            child_str = str(child)
            parent_str = str(parent)
            if child_str in classes and parent_str in classes:
                edges.append((child_str, parent_str, pred))

    # OWL restrictions in subclass axioms:
    # Child rdfs:subClassOf [owl:onProperty REL ; owl:someValuesFrom Parent]
    for child, _, restriction in graph.triples((None, RDFS.subClassOf, None)):
        if not isinstance(child, URIRef) or not isinstance(restriction, BNode):
            continue
        on_prop = graph.value(restriction, OWL.onProperty)
        some_values_from = graph.value(restriction, OWL.someValuesFrom)
        if on_prop in APPROVED_RELATIONS and isinstance(some_values_from, URIRef):
            child_str = str(child)
            parent_str = str(some_values_from)
            if child_str in classes and parent_str in classes:
                edges.append((child_str, parent_str, on_prop))
    return edges


def expand_descendants_from_roots(
    roots: Set[str],
    child_to_parent_edges: List[Tuple[str, str, URIRef]],
) -> Tuple[Set[str], List[Tuple[str, str, URIRef]]]:
    parent_to_children: Dict[str, List[Tuple[str, URIRef]]] = defaultdict(list)
    for child, parent, pred in child_to_parent_edges:
        parent_to_children[parent].append((child, pred))

    visited: Set[str] = set()
    kept_edges: List[Tuple[str, str, URIRef]] = []
    queue: deque[str] = deque()
    for root in roots:
        visited.add(root)
        queue.append(root)

    while queue:
        parent = queue.popleft()
        for child, pred in parent_to_children.get(parent, []):
            kept_edges.append((child, parent, pred))
            if child not in visited:
                visited.add(child)
                queue.append(child)
    return visited, kept_edges


def is_obsolete_owl(graph: Graph, iri: str, label: Optional[str]) -> bool:
    node = URIRef(iri)

    # Common pattern in OWL: owl:deprecated true
    for obj in graph.objects(node, OWL.deprecated):
        if parse_bool_literal(obj):
            return True

    # Other deprecation-like annotations.
    for pred, obj in graph.predicate_objects(node):
        pred_txt = str(pred).lower()
        if "deprecated" in pred_txt and parse_bool_literal(obj):
            return True
        if "obsolete" in pred_txt and parse_bool_literal(obj):
            return True

    if label and label.strip().lower().startswith("obsolete"):
        return True
    return False


def load_owl_graph(path: Path) -> Graph:
    graph = Graph()
    graph.parse(str(path))
    return graph


def process_owl_ontology(
    source_name: str,
    path: Path,
    root_curies: List[str],
    mapping_index: Dict[str, MappingRecord],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    graph = load_owl_graph(path)
    classes, labels, id_to_iri = build_owl_index(graph)
    all_edges = extract_relation_edges(graph, classes)

    root_iris: Set[str] = set()
    unresolved_roots: List[str] = []
    for curie in root_curies:
        iri = id_to_iri.get(curie)
        if not iri:
            iri = curie_to_iri(curie)
        if iri and iri in classes:
            root_iris.add(iri)
        else:
            unresolved_roots.append(curie)

    cam_nodes, cam_edges = expand_descendants_from_roots(root_iris, all_edges)

    kept_nodes: List[Dict[str, Any]] = []
    obsolete_count = 0
    for iri in sorted(cam_nodes):
        curie = iri_to_curie(iri)
        label = labels.get(iri, "")
        if is_obsolete_owl(graph, iri, label):
            obsolete_count += 1
            continue

        mapping = mapping_index.get(curie)
        canonical_id = mapping.canonical_id if mapping else curie
        category = (
            (mapping.category if mapping and mapping.category else infer_biolink_from_id(canonical_id))
            or "biolink:NamedThing"
        )
        kept_nodes.append(
            {
                "ontology": source_name,
                "node_id": curie,
                "node_label": label,
                "canonical_id": canonical_id,
                "category": category,
                "mapping_source": mapping.source_file if mapping else "self",
                "mapping_confidence": mapping.confidence if mapping else None,
            }
        )

    kept_set = {row["node_id"] for row in kept_nodes}
    kept_edges_rows: List[Dict[str, Any]] = []
    for child, parent, pred in cam_edges:
        child_curie = iri_to_curie(child)
        parent_curie = iri_to_curie(parent)
        if child_curie in kept_set and parent_curie in kept_set:
            kept_edges_rows.append(
                {
                    "ontology": source_name,
                    "source_id": child_curie,
                    "target_id": parent_curie,
                    "predicate": normalize_predicate(pred),
                }
            )

    stats = {
        "ontology": source_name,
        "file": str(path),
        "roots_requested": root_curies,
        "roots_resolved": sorted(iri_to_curie(x) for x in root_iris),
        "roots_unresolved": unresolved_roots,
        "nodes_before_prune": len(cam_nodes),
        "nodes_after_prune": len(kept_nodes),
        "edges_after_prune": len(kept_edges_rows),
        "obsolete_pruned": obsolete_count,
    }
    return kept_nodes, kept_edges_rows, stats


def process_ncbitaxon(
    path: Path,
    root_curies: List[str],
    mapping_index: Dict[str, MappingRecord],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    try:
        obonet = importlib.import_module("obonet")
    except ImportError as exc:
        raise ImportError("obonet is required to process ncbitaxon.obo. Install with: pip install obonet") from exc

    graph = obonet.read_obo(str(path))

    parent_to_children: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for child, parent, data in graph.edges(data=True):
        pred = data.get("typedef", "is_a")
        parent_to_children[parent].append((child, pred))

    root_ids: Set[str] = set()
    unresolved_roots: List[str] = []
    for root in root_curies:
        root = normalize_curie(root)
        if root.startswith("NCBITaxon:"):
            node_id = root.split(":", 1)[1]
            if root in graph:
                root_ids.add(root)
            elif node_id in graph:
                root_ids.add(node_id)
            else:
                unresolved_roots.append(root)
        elif root in graph:
            root_ids.add(root)
        else:
            unresolved_roots.append(root)

    visited: Set[str] = set(root_ids)
    queue: deque[str] = deque(root_ids)
    edges: List[Tuple[str, str, str]] = []
    while queue:
        parent = queue.popleft()
        for child, pred in parent_to_children.get(parent, []):
            edges.append((child, parent, pred))
            if child not in visited:
                visited.add(child)
                queue.append(child)

    kept_nodes: List[Dict[str, Any]] = []
    obsolete_count = 0
    for taxid in sorted(visited):
        node = graph.nodes[taxid]
        if str(node.get("is_obsolete", "")).lower() == "true":
            obsolete_count += 1
            continue
        name = node.get("name", "")
        if isinstance(name, str) and name.lower().startswith("obsolete"):
            obsolete_count += 1
            continue

        curie = f"NCBITaxon:{taxid}"
        mapping = mapping_index.get(curie)
        canonical_id = mapping.canonical_id if mapping else curie
        category = (
            (mapping.category if mapping and mapping.category else infer_biolink_from_id(canonical_id))
            or "biolink:OrganismTaxon"
        )
        kept_nodes.append(
            {
                "ontology": "ncbitaxon",
                "node_id": curie,
                "node_label": name,
                "canonical_id": canonical_id,
                "category": category,
                "mapping_source": mapping.source_file if mapping else "self",
                "mapping_confidence": mapping.confidence if mapping else None,
            }
        )

    kept_set = {row["node_id"] for row in kept_nodes}
    kept_edges: List[Dict[str, Any]] = []
    for child, parent, pred in edges:
        child_curie = f"NCBITaxon:{child}"
        parent_curie = f"NCBITaxon:{parent}"
        if child_curie in kept_set and parent_curie in kept_set:
            kept_edges.append(
                {
                    "ontology": "ncbitaxon",
                    "source_id": child_curie,
                    "target_id": parent_curie,
                    "predicate": "rdfs:subClassOf" if pred == "is_a" else pred,
                }
            )

    stats = {
        "ontology": "ncbitaxon",
        "file": str(path),
        "roots_requested": root_curies,
        "roots_resolved": [x if x.startswith("NCBITaxon:") else f"NCBITaxon:{x}" for x in sorted(root_ids)],
        "roots_unresolved": unresolved_roots,
        "nodes_before_prune": len(visited),
        "nodes_after_prune": len(kept_nodes),
        "edges_after_prune": len(kept_edges),
        "obsolete_pruned": obsolete_count,
    }
    return kept_nodes, kept_edges, stats


def write_tsv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def extract_semmed_cam_edges(
    semmed_edge_file: Path,
    cam_canonical_ids: Set[str],
    mapping_index: Dict[str, MappingRecord],
    min_frequency: int,
) -> Tuple[List[Dict[str, Any]], Set[str], Dict[str, Any]]:
    semmed_edges: List[Dict[str, Any]] = []
    semmed_node_ids: Set[str] = set()
    input_node_ids: Set[str] = set()  # unique node IDs in input file (before any filtering)

    stats = {
        "file": str(semmed_edge_file),
        "rows_total": 0,
        "rows_bad": 0,
        "rows_below_min_frequency": 0,
        "rows_kept_cam": 0,
        "min_frequency": min_frequency,
        "input_unique_nodes": 0,
    }

    with open(semmed_edge_file, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stats["rows_total"] += 1

            source_raw = pick_first(row, [":START_ID", "subject", "source", "source_id"])
            target_raw = pick_first(row, [":END_ID", "object", "target", "target_id"])
            pred = pick_first(row, [":TYPE", "predicate", "relation"])
            freq_raw = pick_first(row, ["frequency:int", "frequency", "count"])
            if source_raw:
                input_node_ids.add(str(source_raw).strip())
            if target_raw:
                input_node_ids.add(str(target_raw).strip())
            if not source_raw or not target_raw or not pred:
                stats["rows_bad"] += 1
                continue

            try:
                freq = int(str(freq_raw)) if freq_raw not in (None, "") else 1
            except ValueError:
                freq = 1
            if freq < min_frequency:
                stats["rows_below_min_frequency"] += 1
                continue

            source_id = canonicalize_id(str(source_raw), mapping_index)
            target_id = canonicalize_id(str(target_raw), mapping_index)
            if source_id in cam_canonical_ids and target_id in cam_canonical_ids:
                semmed_edges.append(
                    {
                        "ontology": "semmeddb",
                        "source_id": source_id,
                        "predicate": str(pred),
                        "target_id": target_id,
                    }
                )
                semmed_node_ids.add(source_id)
                semmed_node_ids.add(target_id)
                stats["rows_kept_cam"] += 1

    stats["input_unique_nodes"] = len(input_node_ids)
    return semmed_edges, semmed_node_ids, stats


def ensure_semmed_nodes_present(
    deduped_nodes: Dict[str, Dict[str, Any]],
    semmed_node_ids: Set[str],
    mapping_index: Dict[str, MappingRecord],
) -> int:
    added = 0
    for node_id in semmed_node_ids:
        if node_id in deduped_nodes:
            continue
        mapping = mapping_index.get(node_id)
        category = mapping.category if mapping and mapping.category else infer_biolink_from_id(node_id)
        deduped_nodes[node_id] = {
            "ontology": "semmeddb",
            "node_id": node_id,
            "node_label": mapping.label if mapping and mapping.label else "",
            "canonical_id": node_id,
            "category": category,
            "mapping_source": mapping.source_file if mapping else "self",
            "mapping_confidence": mapping.confidence if mapping else None,
        }
        added += 1
    return added


def load_ikraph_node_dict(path: Optional[Path]) -> Dict[str, str]:
    """Build biokdeid -> CURIE from NER_ID_dict_cap_final.json (list of {biokdeid, id})."""
    if not path or not path.exists():
        return {}
    out: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        return {}
    for item in data:
        if not isinstance(item, dict):
            continue
        bid = item.get("biokdeid")
        eid = item.get("id")
        if bid is not None and eid and str(eid).strip().upper() != "NA":
            out[str(bid).strip()] = str(eid).strip()
    return out


def extract_ikraph_cam_edges(
    ikraph_edge_file: Path,
    cam_canonical_ids: Set[str],
    mapping_index: Dict[str, MappingRecord],
    ikraph_node_dict: Optional[Dict[str, str]] = None,
) -> Tuple[List[Dict[str, Any]], Set[str], Dict[str, Any]]:
    """Extract iKraph edges whose endpoints normalize to CAM canonical IDs.
    If ikraph_node_dict is provided, node_one_id/node_two_id (biokdeid) are resolved to CURIEs first."""
    ikraph_edges: List[Dict[str, Any]] = []
    ikraph_node_ids: Set[str] = set()
    id_map = ikraph_node_dict or {}
    input_node_ids: Set[str] = set()  # unique node IDs in input file (before any filtering)
    stats = {
        "file": str(ikraph_edge_file),
        "rows_total": 0,
        "rows_bad": 0,
        "rows_kept_cam": 0,
        "input_unique_nodes": 0,
    }
    with open(ikraph_edge_file, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stats["rows_total"] += 1
            source_raw = pick_first(row, [":START_ID", "node_one_id", "subject", "source", "source_id"])
            target_raw = pick_first(row, [":END_ID", "node_two_id", "object", "target", "target_id"])
            pred = pick_first(row, [":TYPE", "predicate_label", "relationship_type", "predicate", "relation"])
            if source_raw:
                input_node_ids.add(str(source_raw).strip())
            if target_raw:
                input_node_ids.add(str(target_raw).strip())
            if not source_raw or not target_raw or not pred:
                stats["rows_bad"] += 1
                continue
            source_raw = str(source_raw).strip()
            target_raw = str(target_raw).strip()
            if id_map:
                source_raw = id_map.get(source_raw, source_raw)
                target_raw = id_map.get(target_raw, target_raw)
            source_id = canonicalize_id(source_raw, mapping_index)
            target_id = canonicalize_id(target_raw, mapping_index)
            if source_id in cam_canonical_ids and target_id in cam_canonical_ids:
                ikraph_edges.append(
                    {
                        "ontology": "ikraph",
                        "source_id": source_id,
                        "predicate": str(pred),
                        "target_id": target_id,
                    }
                )
                ikraph_node_ids.add(source_id)
                ikraph_node_ids.add(target_id)
                stats["rows_kept_cam"] += 1
    stats["input_unique_nodes"] = len(input_node_ids)
    return ikraph_edges, ikraph_node_ids, stats


def ensure_ikraph_nodes_present(
    deduped_nodes: Dict[str, Dict[str, Any]],
    ikraph_node_ids: Set[str],
    mapping_index: Dict[str, MappingRecord],
) -> int:
    added = 0
    for node_id in ikraph_node_ids:
        if node_id in deduped_nodes:
            continue
        mapping = mapping_index.get(node_id)
        category = mapping.category if mapping and mapping.category else infer_biolink_from_id(node_id)
        deduped_nodes[node_id] = {
            "ontology": "ikraph",
            "node_id": node_id,
            "node_label": mapping.label if mapping and mapping.label else "",
            "canonical_id": node_id,
            "category": category,
            "mapping_source": mapping.source_file if mapping else "self",
            "mapping_confidence": mapping.confidence if mapping else None,
        }
        added += 1
    return added


def write_analysis_doc(
    doc_path: Path,
    stats: Dict[str, Any],
    node_counts_by_ontology: Dict[str, int],
    edge_counts_by_ontology: Dict[str, int],
    relaxed_overlap_summary: Optional[Dict[str, Any]],
    semmed_build_summary: Optional[Dict[str, Any]],
) -> None:
    total_nodes = stats.get("final_counts", {}).get("unique_nodes", 0)
    total_edges = stats.get("final_counts", {}).get("unique_edges", 0)
    by_source = stats.get("by_source", {})
    by_source_nodes = by_source.get("nodes", {})
    by_source_edges = by_source.get("edges", {})
    semmed_edges = int(by_source_edges.get("semmeddb", 0))
    ikraph_edges = int(by_source_edges.get("ikraph", 0))
    semmed_nodes = int(by_source_nodes.get("semmeddb", 0))
    ikraph_nodes = int(by_source_nodes.get("ikraph", 0))
    overlap = stats.get("semmeddb_ikraph_overlap", {})
    nodes_in_both = int(overlap.get("nodes_in_both", 0))
    semmed_endpoint_nodes = int(overlap.get("semmeddb_endpoint_nodes", 0))
    ikraph_endpoint_nodes = int(overlap.get("ikraph_endpoint_nodes", 0))
    semmed_input = stats.get("semmeddb", {})
    ikraph_input = stats.get("ikraph", {})
    semmed_input_edges = int(semmed_input.get("rows_total", 0))
    semmed_input_nodes = int(semmed_input.get("input_unique_nodes", 0))
    ikraph_input_edges = int(ikraph_input.get("rows_total", 0))
    ikraph_input_nodes = int(ikraph_input.get("input_unique_nodes", 0))

    ontology_edges = total_edges - semmed_edges - ikraph_edges
    ontology_nodes = total_nodes - semmed_nodes - ikraph_nodes
    semmed_edge_pct = (100.0 * semmed_edges / total_edges) if total_edges else 0.0
    ikraph_edge_pct = (100.0 * ikraph_edges / total_edges) if total_edges else 0.0
    ontology_edge_pct = (100.0 * ontology_edges / total_edges) if total_edges else 0.0

    lines: List[str] = []
    lines.append("# CAM Subgraph: SemMedDB + iKraph Union (Auto-generated)")
    lines.append("")
    lines.append("## High-level overview of the process")
    lines.append("")
    lines.append("This pipeline **unions SemMedDB and iKraph** and keeps only **CAM-related** nodes and edges.")
    lines.append("")
    lines.append("1. **CAM ontology scope** — Load authoritative roots (FoodOn, TCDO, LSFO, ChEBI, NCBITaxon), expand by closure (subClassOf / part_of / has_role), and prune obsolete terms. This defines the set of *CAM canonical IDs*.")
    lines.append("2. **Normalization** — Optional mapping files (e.g. NodeNorm, normalized SemMedDB/iKraph) map input identifiers to these canonical IDs.")
    lines.append("3. **Union and filter** — SemMedDB and iKraph edge files are read; only edges whose **both endpoints** normalize to a CAM canonical ID are kept. Nodes are added as needed. Ontology-derived edges (from step 1) and retained SemMedDB/iKraph edges form a single union graph.")
    lines.append("4. **Output** — One node table, one edge table, and stats (including breakdown by source: ontologies, SemMedDB, iKraph).")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Summary counts")
    lines.append("")
    lines.append(f"| Metric | Count |")
    lines.append(f"|--------|-------|")
    lines.append(f"| **Total CAM nodes** | {total_nodes:,} |")
    lines.append(f"| **Total CAM edges** | {total_edges:,} |")
    lines.append(f"| Edges from ontologies | {ontology_edges:,} ({ontology_edge_pct:.1f}%) |")
    lines.append(f"| Edges from SemMedDB | {semmed_edges:,} ({semmed_edge_pct:.1f}%) |")
    lines.append(f"| Edges from iKraph | {ikraph_edges:,} ({ikraph_edge_pct:.1f}%) |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Input context (before filtering)")
    lines.append("")
    lines.append("Raw counts from the SemMedDB and iKraph edge files **before** normalization and CAM filtering. These give context for how much of each source is retained.")
    lines.append("")
    lines.append("| Source | Input edges | Input unique nodes | In CAM (edges) | In CAM (endpoint nodes) |")
    lines.append("|--------|-------------|--------------------|----------------|-------------------------|")
    if semmed_input_edges or semmed_input_nodes:
        lines.append(f"| SemMedDB | {semmed_input_edges:,} | {semmed_input_nodes:,} | {semmed_edges:,} | {semmed_endpoint_nodes:,} |")
    if ikraph_input_edges or ikraph_input_nodes:
        lines.append(f"| iKraph | {ikraph_input_edges:,} | {ikraph_input_nodes:,} | {ikraph_edges:,} | {ikraph_endpoint_nodes:,} |")
    if semmed_input_edges or ikraph_input_edges:
        lines.append("")
        if semmed_input_edges:
            pct_sem = (100.0 * semmed_edges / semmed_input_edges) if semmed_input_edges else 0.0
            lines.append(f"- SemMedDB: **{semmed_edges:,} / {semmed_input_edges:,}** edges retained ({pct_sem:.2f}%); **{semmed_endpoint_nodes:,}** unique endpoint nodes in CAM (from **{semmed_input_nodes:,}** in input).")
        if ikraph_input_edges:
            pct_ik = (100.0 * ikraph_edges / ikraph_input_edges) if ikraph_input_edges else 0.0
            lines.append(f"- iKraph: **{ikraph_edges:,} / {ikraph_input_edges:,}** edges retained ({pct_ik:.2f}%); **{ikraph_endpoint_nodes:,}** unique endpoint nodes in CAM (from **{ikraph_input_nodes:,}** in input).")
    elif not (semmed_input_edges or semmed_input_nodes or ikraph_input_edges or ikraph_input_nodes):
        lines.append("")
        lines.append("No SemMedDB or iKraph input edge files were processed.")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 1. Equivalent nodes merged (SemMedDB ∩ iKraph)")
    lines.append("")
    lines.append("Nodes that appear as endpoints of **both** SemMedDB and iKraph edges in the final graph (canonical IDs shared by both sources).")
    lines.append("")
    lines.append(f"- SemMedDB endpoint nodes (unique): `{semmed_endpoint_nodes:,}`")
    lines.append(f"- iKraph endpoint nodes (unique): `{ikraph_endpoint_nodes:,}`")
    lines.append(f"- **Nodes in both (merged overlap): `{nodes_in_both:,}`**")
    if semmed_endpoint_nodes or ikraph_endpoint_nodes:
        pct_semmed = (100.0 * nodes_in_both / semmed_endpoint_nodes) if semmed_endpoint_nodes else 0.0
        pct_ikraph = (100.0 * nodes_in_both / ikraph_endpoint_nodes) if ikraph_endpoint_nodes else 0.0
        lines.append(f"- Share of SemMedDB endpoint nodes that also appear in iKraph: `{pct_semmed:.1f}%`")
        lines.append(f"- Share of iKraph endpoint nodes that also appear in SemMedDB: `{pct_ikraph:.1f}%`")
    lines.append("")
    lines.append("```mermaid")
    lines.append("flowchart LR")
    lines.append(f'  A["SemMedDB endpoint nodes<br/>{semmed_endpoint_nodes:,}"] --> C["Nodes in both<br/>{nodes_in_both:,}"]')
    lines.append(f'  B["iKraph endpoint nodes<br/>{ikraph_endpoint_nodes:,}"] --> C')
    lines.append("```")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 2. Amount of graph from SemMedDB vs iKraph")
    lines.append("")
    lines.append("### Edges by source (SemMedDB vs iKraph vs ontologies)")
    lines.append("")
    lines.append("```mermaid")
    lines.append("pie showData")
    lines.append("title CAM edges by source")
    if ontology_edges > 0:
        lines.append(f'  "Ontologies (FoodOn/TCDO/LSFO/ChEBI/NCBITaxon)" : {ontology_edges}')
    if semmed_edges > 0:
        lines.append(f'  "SemMedDB" : {semmed_edges}')
    if ikraph_edges > 0:
        lines.append(f'  "iKraph" : {ikraph_edges}')
    if ontology_edges == 0 and semmed_edges == 0 and ikraph_edges == 0:
        lines.append('  "No edges" : 1')
    lines.append("```")
    lines.append("")
    lines.append("### Nodes contributed by source")
    lines.append("")
    lines.append("**Why SemMedDB and iKraph show few \"attributed\" nodes:** Most nodes that appear as endpoints of SemMedDB or iKraph edges are already in the graph from the ontology expansion (e.g. CHEBI). We only attribute a node to SemMedDB or iKraph when it was *newly added* because it was an edge endpoint but not already present. So the pie below reflects \"primary source\" attribution; the real participation is in the *Endpoint nodes* row in section 5.")
    lines.append("")
    lines.append("```mermaid")
    lines.append("pie showData")
    lines.append("title CAM nodes by primary source (attribution)")
    for key in sorted(node_counts_by_ontology.keys()):
        v = node_counts_by_ontology[key]
        if v > 0:
            lines.append(f'  "{key}" : {v}')
    if not any(node_counts_by_ontology.values()):
        lines.append('  "No nodes" : 1')
    lines.append("```")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 3. Ontologies in the final graph")
    lines.append("")
    lines.append("Ontology-derived nodes and edges (before adding SemMedDB/iKraph).")
    lines.append("")
    lines.append("### Ontology node counts")
    lines.append("")
    lines.append("```mermaid")
    lines.append("pie showData")
    lines.append("title Nodes by ontology")
    for key in sorted(edge_counts_by_ontology.keys()):
        if key not in ("semmeddb", "ikraph"):
            v = node_counts_by_ontology.get(key, 0)
            if v > 0:
                lines.append(f'  "{key}" : {v}')
    ont_node_total = sum(node_counts_by_ontology.get(k, 0) for k in edge_counts_by_ontology if k not in ("semmeddb", "ikraph"))
    if ont_node_total == 0:
        lines.append('  "None" : 1')
    lines.append("```")
    lines.append("")
    lines.append("### Ontology edge counts")
    lines.append("")
    lines.append("```mermaid")
    lines.append("pie showData")
    lines.append("title Edges by ontology")
    for key in sorted(edge_counts_by_ontology.keys()):
        if key not in ("semmeddb", "ikraph"):
            v = edge_counts_by_ontology.get(key, 0)
            if v > 0:
                lines.append(f'  "{key}" : {v}')
    ont_edge_total = sum(edge_counts_by_ontology.get(k, 0) for k in edge_counts_by_ontology if k not in ("semmeddb", "ikraph"))
    if ont_edge_total == 0:
        lines.append('  "None" : 1')
    lines.append("```")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 4. Pipeline flowchart")
    lines.append("")
    lines.append("```mermaid")
    lines.append("flowchart TB")
    lines.append("  subgraph inputs[Inputs]")
    lines.append('    OWL["Ontology files<br/>(FoodOn, TCDO, LSFO, ChEBI, NCBITaxon)"]')
    lines.append('    SEM["SemMedDB edge file"]')
    lines.append('    IKR["iKraph edge file"]')
    lines.append('    MAP["Normalized mapping files"]')
    lines.append("  end")
    lines.append("  subgraph step1[Step 1: CAM scope]")
    lines.append('    OWL --> ROOTS["Expand from roots"]')
    lines.append('    ROOTS --> CAM["CAM canonical IDs"]')
    lines.append("  end")
    lines.append("  subgraph step2[Step 2: Union & filter]")
    lines.append('    CAM --> FILTER["Keep edges with both endpoints in CAM"]')
    lines.append('    SEM --> FILTER')
    lines.append('    IKR --> FILTER')
    lines.append('    MAP --> FILTER')
    lines.append('    FILTER --> UNION["Union graph<br/>(ontology + SemMedDB + iKraph)"]')
    lines.append("  end")
    lines.append("  subgraph out[Output]")
    lines.append('    UNION --> NODES["cam_nodes.tsv"]')
    lines.append('    UNION --> EDGES["cam_edges.tsv"]')
    lines.append('    UNION --> STATS["cam_stats.json"]')
    lines.append("  end")
    lines.append("```")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 5. By-source breakdown (reference)")
    lines.append("")
    lines.append("Counts below match the Summary above.")
    lines.append("")
    lines.append("- **Nodes (attributed):** Nodes whose primary source in the output is that pipeline. SemMedDB and iKraph are small here because most of their edge endpoints are already in the graph from ontologies (e.g. CHEBI).")
    lines.append("- **Endpoint nodes:** Unique nodes that appear in at least one edge from that source (only shown for SemMedDB and iKraph).")
    lines.append("- **Edges:** Edges from that source (after deduplication).")
    lines.append("")
    lines.append("| Source | Nodes (attributed) | Endpoint nodes | Edges |")
    lines.append("|--------|-------------------|----------------|-------|")
    sum_nodes = 0
    sum_edges = 0
    for s in sorted(by_source_nodes.keys()):
        n = by_source_nodes.get(s, 0)
        e = by_source_edges.get(s, 0)
        sum_nodes += n
        sum_edges += e
        if s == "semmeddb":
            ep = semmed_endpoint_nodes
        elif s == "ikraph":
            ep = ikraph_endpoint_nodes
        else:
            ep = ""  # ontology: endpoint count not separately tracked
        ep_str = f"{ep:,}" if isinstance(ep, int) else "—"
        lines.append(f"| {s} | {n:,} | {ep_str} | {e:,} |")
    lines.append(f"| **Total** | **{sum_nodes:,}** | — | **{sum_edges:,}** |")
    lines.append("")
    if relaxed_overlap_summary:
        relaxed_overlap_edges = relaxed_overlap_summary.get("overlap_edges")
        relaxed_overlap_indexed = relaxed_overlap_summary.get("semmed_edges_indexed")
        if relaxed_overlap_edges is not None and relaxed_overlap_indexed is not None and relaxed_overlap_indexed:
            overlap_rate = 100.0 * relaxed_overlap_edges / relaxed_overlap_indexed
            lines.append("## SemMedDB vs iKraph overlap (relaxed index)")
            lines.append("")
            lines.append(f"- Indexed SemMed endpoint pairs: `{relaxed_overlap_indexed:,}`")
            lines.append(f"- Relaxed overlap edges: `{relaxed_overlap_edges:,}` ({overlap_rate:.2f}%)")
            lines.append("")
    if semmed_build_summary:
        semmed_mapped_rows = semmed_build_summary.get("mapped_rows")
        semmed_total_rows = semmed_build_summary.get("total_predication_rows")
        if semmed_total_rows:
            mapped_rate = 100.0 * (semmed_mapped_rows or 0) / semmed_total_rows
            lines.append("## SemMedDB ingestion coverage")
            lines.append("")
            lines.append(f"- Mapped rows: `{semmed_mapped_rows or 0:,}` / `{semmed_total_rows:,}` ({mapped_rate:.2f}%)")
            lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*Report generated by `cam_subgraph_extract.py`.*")
    lines.append("")

    with open(doc_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def main() -> None:
    args = parse_args()
    ontology_dir = Path(args.ontology_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    roots = load_roots(args.roots_json)
    mapping_index = load_mapping_index(args.normalized_files, args.min_confidence, args.require_confidence)

    all_nodes: List[Dict[str, Any]] = []
    all_edges: List[Dict[str, Any]] = []
    stats: Dict[str, Any] = {
        "config": {
            "ontology_dir": str(ontology_dir),
            "roots_json": args.roots_json,
            "normalized_files": args.normalized_files,
            "min_confidence": args.min_confidence,
            "require_confidence": args.require_confidence,
            "semmed_edge_file": args.semmed_edge_file,
            "semmed_min_frequency": args.semmed_min_frequency,
            "ikraph_edge_file": args.ikraph_edge_file,
            "ikraph_node_dict": args.ikraph_node_dict,
            "analysis_doc": args.analysis_doc,
            "overlap_relaxed_summary": args.overlap_relaxed_summary,
            "semmed_summary": args.semmed_summary,
        },
        "mapping_index_size": len(mapping_index),
        "ontologies": [],
        "missing_ontology_files": [],
    }

    for source_name, filename in ONTOLOGY_FILENAMES.items():
        ontology_path = ontology_dir / filename
        if not ontology_path.exists():
            stats["missing_ontology_files"].append(str(ontology_path))
            print(f"[warn] Missing ontology file: {ontology_path}")
            continue

        source_roots = roots.get(source_name, [])
        if not source_roots:
            print(f"[warn] No roots configured for {source_name}, skipping this ontology.")
            stats["ontologies"].append(
                {
                    "ontology": source_name,
                    "file": str(ontology_path),
                    "skipped": True,
                    "reason": "no_roots_configured",
                }
            )
            continue

        print(f"[info] Processing {source_name} from {ontology_path} ...")
        try:
            if source_name == "ncbitaxon":
                nodes, edges, source_stats = process_ncbitaxon(ontology_path, source_roots, mapping_index)
            else:
                nodes, edges, source_stats = process_owl_ontology(source_name, ontology_path, source_roots, mapping_index)
        except Exception as exc:  # pragma: no cover - robust handling for malformed ontology files
            print(f"[warn] Failed processing {source_name}: {exc}")
            stats["ontologies"].append(
                {
                    "ontology": source_name,
                    "file": str(ontology_path),
                    "skipped": True,
                    "reason": "processing_error",
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
            )
            continue

        all_nodes.extend(nodes)
        all_edges.extend(edges)
        source_stats["skipped"] = False
        stats["ontologies"].append(source_stats)

    deduped_nodes: Dict[str, Dict[str, Any]] = {}
    for row in all_nodes:
        key = row["node_id"]
        if key not in deduped_nodes:
            deduped_nodes[key] = row
        else:
            # Keep the row with higher confidence if both exist.
            current = deduped_nodes[key]
            curr_conf = current.get("mapping_confidence")
            new_conf = row.get("mapping_confidence")
            curr_rank = -1.0 if curr_conf is None else float(curr_conf)
            new_rank = -1.0 if new_conf is None else float(new_conf)
            if new_rank > curr_rank:
                deduped_nodes[key] = row

    # Optional SemMedDB edge integration:
    # keep SemMedDB edges only when both endpoints resolve to CAM canonical IDs.
    semmed_stats = None
    if args.semmed_edge_file:
        semmed_path = Path(args.semmed_edge_file)
        if semmed_path.exists():
            cam_canonical_ids = {row["canonical_id"] for row in deduped_nodes.values()}
            semmed_edges, semmed_node_ids, semmed_stats = extract_semmed_cam_edges(
                semmed_path,
                cam_canonical_ids,
                mapping_index,
                args.semmed_min_frequency,
            )
            all_edges.extend(semmed_edges)
            added_nodes = ensure_semmed_nodes_present(deduped_nodes, semmed_node_ids, mapping_index)
            semmed_stats["semmed_nodes_added"] = added_nodes
            semmed_stats["semmed_edges_added"] = len(semmed_edges)
            print(
                f"[info] Added SemMedDB CAM edges: {len(semmed_edges):,} "
                f"(nodes added: {added_nodes:,}) from {semmed_path}"
            )
        else:
            semmed_stats = {"file": str(semmed_path), "skipped": True, "reason": "missing_file"}
            print(f"[warn] SemMedDB edge file not found, skipping: {semmed_path}")

    # Optional iKraph edge integration (same as SemMedDB: keep edges whose endpoints are in CAM).
    ikraph_stats = None
    ikraph_node_dict: Dict[str, str] = {}
    if getattr(args, "ikraph_node_dict", None):
        nd_path = Path(args.ikraph_node_dict)
        if nd_path.exists():
            print(f"[info] Loading iKraph node dict from {nd_path} ...")
            ikraph_node_dict = load_ikraph_node_dict(nd_path)
            print(f"[info] iKraph node dict: {len(ikraph_node_dict):,} biokdeid -> CURIE mappings")
    if args.ikraph_edge_file:
        ikraph_path = Path(args.ikraph_edge_file)
        if ikraph_path.exists():
            cam_canonical_ids = {row["canonical_id"] for row in deduped_nodes.values()}
            ikraph_edges, ikraph_node_ids, ikraph_stats = extract_ikraph_cam_edges(
                ikraph_path, cam_canonical_ids, mapping_index, ikraph_node_dict or None
            )
            all_edges.extend(ikraph_edges)
            added_ikraph_nodes = ensure_ikraph_nodes_present(deduped_nodes, ikraph_node_ids, mapping_index)
            ikraph_stats["ikraph_nodes_added"] = added_ikraph_nodes
            ikraph_stats["ikraph_edges_added"] = len(ikraph_edges)
            print(
                f"[info] Added iKraph CAM edges: {len(ikraph_edges):,} "
                f"(nodes added: {added_ikraph_nodes:,}) from {ikraph_path}"
            )
        else:
            ikraph_stats = {"file": str(ikraph_path), "skipped": True, "reason": "missing_file"}
            print(f"[warn] iKraph edge file not found, skipping: {ikraph_path}")

    nodes_out = sorted(deduped_nodes.values(), key=lambda x: x["node_id"])
    node_counts_by_ontology: Dict[str, int] = defaultdict(int)
    for row in nodes_out:
        node_counts_by_ontology[row["ontology"]] += 1
    edge_keys = set()
    edges_out: List[Dict[str, Any]] = []
    edge_counts_by_ontology: Dict[str, int] = defaultdict(int)
    for row in all_edges:
        key = (row["source_id"], row["predicate"], row["target_id"])
        if key not in edge_keys:
            edge_keys.add(key)
            edges_out.append(row)
            edge_counts_by_ontology[row["ontology"]] += 1

    write_tsv(
        output_dir / "cam_nodes.tsv",
        nodes_out,
        [
            "ontology",
            "node_id",
            "node_label",
            "canonical_id",
            "category",
            "mapping_source",
            "mapping_confidence",
        ],
    )
    write_tsv(
        output_dir / "cam_edges.tsv",
        edges_out,
        ["ontology", "source_id", "predicate", "target_id"],
    )

    stats["final_counts"] = {
        "unique_nodes": len(nodes_out),
        "unique_edges": len(edges_out),
    }
    if semmed_stats is not None:
        stats["semmeddb"] = semmed_stats
    if ikraph_stats is not None:
        stats["ikraph"] = ikraph_stats
    # Explicit breakdown by source (semmeddb and ikraph always present for reporting).
    all_sources = set(node_counts_by_ontology.keys()) | set(edge_counts_by_ontology.keys()) | {"semmeddb", "ikraph"}
    stats["by_source"] = {
        "nodes": {s: node_counts_by_ontology.get(s, 0) for s in sorted(all_sources)},
        "edges": {s: edge_counts_by_ontology.get(s, 0) for s in sorted(all_sources)},
    }
    # Nodes that appear as endpoints of both SemMedDB and iKraph edges (merged overlap).
    semmeddb_endpoint_nodes: Set[str] = set()
    ikraph_endpoint_nodes: Set[str] = set()
    for row in edges_out:
        src, tgt = row["source_id"], row["target_id"]
        if row["ontology"] == "semmeddb":
            semmeddb_endpoint_nodes.add(src)
            semmeddb_endpoint_nodes.add(tgt)
        elif row["ontology"] == "ikraph":
            ikraph_endpoint_nodes.add(src)
            ikraph_endpoint_nodes.add(tgt)
    stats["semmeddb_ikraph_overlap"] = {
        "nodes_in_both": len(semmeddb_endpoint_nodes & ikraph_endpoint_nodes),
        "semmeddb_endpoint_nodes": len(semmeddb_endpoint_nodes),
        "ikraph_endpoint_nodes": len(ikraph_endpoint_nodes),
    }
    with open(output_dir / "cam_stats.json", "w", encoding="utf-8") as handle:
        json.dump(stats, handle, indent=2, ensure_ascii=False)

    # Auto-generated analysis document (default: output_dir/CAM_ANALYSIS.md).
    if not args.no_analysis_doc and args.analysis_doc:
        relaxed_summary = None
        semmed_summary = None
        if args.overlap_relaxed_summary:
            summary_path = Path(args.overlap_relaxed_summary)
            if summary_path.exists():
                with open(summary_path, "r", encoding="utf-8") as handle:
                    relaxed_summary = json.load(handle)
        if args.semmed_summary:
            semmed_path = Path(args.semmed_summary)
            if semmed_path.exists():
                with open(semmed_path, "r", encoding="utf-8") as handle:
                    semmed_summary = json.load(handle)
        analysis_path = Path(args.analysis_doc)
        if not analysis_path.is_absolute():
            analysis_path = output_dir / analysis_path
        write_analysis_doc(
            analysis_path,
            stats,
            dict(node_counts_by_ontology),
            dict(edge_counts_by_ontology),
            relaxed_summary,
            semmed_summary,
        )
        print(f"[done] Wrote analysis doc: {analysis_path}")

    print(f"[done] Wrote CAM nodes: {output_dir / 'cam_nodes.tsv'} ({len(nodes_out):,})")
    print(f"[done] Wrote CAM edges: {output_dir / 'cam_edges.tsv'} ({len(edges_out):,})")
    print(f"[done] Wrote stats: {output_dir / 'cam_stats.json'}")


if __name__ == "__main__":
    main()
