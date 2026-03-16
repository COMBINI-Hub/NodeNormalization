#!/usr/bin/env python3
"""
Extract a COMBO-focused subgraph from normalized SemMedDB + iKraph.

Inputs
- Normalized maps:
    - semmed_ikraph_normalized/semmed_normalized_full_with_pubchem.json
    - semmed_ikraph_normalized/ikraph_normalized_full.with_newly_normalized_20260123_v6_20260218.json
- Full node sources:
    - neo4jexploration/semmed_data/concept.csv
    - neo4jexploration/iKraph_raw/iKraph_full/NER_ID_dict_cap_final.json
- SKOS-enriched COMBO ontology:
    - COMBO_20260115_with_skos.owl

Outputs (default: semmed_ikraph_normalized/combo_subgraph)
- combo_seed_nodes.tsv
- combo_normalization_links.tsv
- semmed_edges_seed_adjacent.tsv
- semmed_edges_seed_induced.tsv
- ikraph_edges_seed_adjacent.tsv
- ikraph_edges_seed_induced.tsv
- combo_subgraph_summary.json
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from rdflib import BNode, Graph, Namespace, URIRef
from rdflib.namespace import RDFS

try:
    import ijson
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "Missing dependency 'ijson'. Install with: python3 -m pip install ijson"
    ) from e


SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
COMBO_NS = "https://github.com/Tao-AI-group/COMBINI#"
UMLS_CUI_PRED = URIRef(f"{COMBO_NS}UMLS_CUI")
AMBIGUOUS_TOKEN_MAX_LEN = 4
NON_NATIVE_TERM_EMBEDDING_MIN = 0.80


def normalize_text(text: str) -> str:
    text = html.unescape(text or "").lower()
    text = re.sub(r"[_\-/]", " ", text)
    text = re.sub(r"[^a-z0-9 ]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_ambiguous_short_token(term: str) -> bool:
    term = (term or "").strip().lower()
    if not term or " " in term:
        return False
    if len(term) > AMBIGUOUS_TOKEN_MAX_LEN:
        return False
    return bool(re.fullmatch(r"[a-z0-9]+", term))


def source_code_to_curie(source: str, code: str) -> Optional[str]:
    source = (source or "").upper()
    code = (code or "").strip()
    if not code:
        return None
    if source.startswith("MSH") or source == "MESH":
        return f"MESH:{code}"
    if source.startswith("SNOMED"):
        return f"SNOMEDCT:{code}"
    if source == "RXNORM":
        return f"RXNORM:{code}"
    if source == "NCI":
        return f"NCIT:{code}"
    if source == "DRUGBANK":
        return f"DRUGBANK:{code}"
    if source == "PUBCHEM":
        return f"PubChem:CID{code}"
    if source == "UMLS":
        return f"UMLS:{code}"
    return None


def identifiers_org_to_curie(uri: str) -> Optional[str]:
    m = re.search(r"identifiers\.org/([^/]+)/(.+)$", uri)
    if not m:
        return None
    namespace = m.group(1).lower()
    code = m.group(2).strip()
    if not code:
        return None

    if namespace == "umls":
        return f"UMLS:{code}"
    if namespace == "mesh":
        return f"MESH:{code}"
    if namespace == "rxnorm":
        return f"RXNORM:{code}"
    if namespace == "snomedct":
        code = code.rstrip("/")
        if "/" in code:
            code = code.split("/")[-1]
        return f"SNOMEDCT:{code}"
    if namespace == "pubchem.compound":
        if not code.upper().startswith("CID"):
            code = f"CID{code}"
        return f"PubChem:{code}"
    if namespace in {"ncbigene", "ncbi.gene"}:
        return f"NCBIGene:{code}"
    if namespace in {"taxonomy", "ncbitaxon"}:
        return f"NCBITaxon:{code}"
    if namespace == "chebi":
        return f"CHEBI:{code}"

    return None


def normalize_identifier(raw: str) -> Optional[str]:
    raw = (raw or "").strip()
    if not raw or raw == "NA":
        return None

    if raw.startswith("http://") or raw.startswith("https://"):
        curie = identifiers_org_to_curie(raw)
        if curie:
            return curie
        m = re.search(r"/source/([^/]+)/([^/?#]+)$", raw)
        if m:
            curie = source_code_to_curie(m.group(1), m.group(2))
            if curie:
                return curie
        return None

    if re.fullmatch(r"C\d{7}", raw):
        return f"UMLS:{raw}"

    if ":" not in raw:
        return raw

    prefix, local = raw.split(":", 1)
    prefix_upper = prefix.upper()
    local = local.strip()

    if prefix_upper == "UMLS":
        return f"UMLS:{local}"
    if prefix_upper == "MESH":
        return f"MESH:{local}"
    if prefix_upper == "NCBI":
        return f"NCBIGene:{local}"
    if prefix_upper in {"NCBIGENE", "NCBI_GENE"}:
        return f"NCBIGene:{local}"
    if prefix_upper in {"NCBITAXID", "NCBITAXON"}:
        return f"NCBITaxon:{local}"
    if prefix_upper == "PUBCHEM":
        if not local.upper().startswith("CID"):
            local = f"CID{local}"
        return f"PubChem:{local}"
    if prefix_upper == "PUBCHEM.COMPOUND":
        if not local.upper().startswith("CID"):
            local = f"CID{local}"
        return f"PubChem:{local}"
    if prefix_upper == "IKRAPH":
        return f"IKRAPH:{local}"
    return f"{prefix}:{local}"


def parse_combo_ontology(owl_path: Path):
    g = Graph()
    g.parse(str(owl_path))

    combo_uri_to_terms: Dict[str, Set[str]] = defaultdict(set)
    combo_uri_to_native_terms: Dict[str, Set[str]] = defaultdict(set)
    combo_uri_to_ids: Dict[str, Set[str]] = defaultdict(set)
    combo_uri_to_locked_umls: Dict[str, Set[str]] = defaultdict(set)
    combo_uri_to_label_terms: Dict[str, Set[str]] = defaultdict(set)

    for combo_uri_ref, _, lit in g.triples((None, SKOS.prefLabel, None)):
        combo_uri = str(combo_uri_ref)
        if not combo_uri.startswith(COMBO_NS):
            continue
        term = normalize_text(str(lit))
        if term:
            combo_uri_to_terms[combo_uri].add(term)
            combo_uri_to_native_terms[combo_uri].add(term)

    for combo_uri_ref, _, lit in g.triples((None, SKOS.altLabel, None)):
        combo_uri = str(combo_uri_ref)
        if not combo_uri.startswith(COMBO_NS):
            continue
        term = normalize_text(str(lit))
        if term:
            combo_uri_to_terms[combo_uri].add(term)
            combo_uri_to_native_terms[combo_uri].add(term)

    # Include canonical labels from RDF schema labels (present on class resources).
    for combo_uri_ref, _, lit in g.triples((None, RDFS.label, None)):
        combo_uri = str(combo_uri_ref)
        if not combo_uri.startswith(COMBO_NS):
            continue
        term = normalize_text(str(lit))
        if term:
            combo_uri_to_terms[combo_uri].add(term)
            combo_uri_to_native_terms[combo_uri].add(term)
            combo_uri_to_label_terms[combo_uri].add(term)

    # Include explicit UMLS CUI annotations from the ontology.
    for combo_uri_ref, _, lit in g.triples((None, UMLS_CUI_PRED, None)):
        combo_uri = str(combo_uri_ref)
        if not combo_uri.startswith(COMBO_NS):
            continue
        curie = normalize_identifier(str(lit))
        if curie:
            combo_uri_to_ids[combo_uri].add(curie)
            if curie.startswith("UMLS:"):
                combo_uri_to_locked_umls[combo_uri].add(curie)

    # Pull external IDs directly from source comments (UMLS/MeSH/etc URLs).
    for combo_uri_ref, _, lit in g.triples((None, RDFS.comment, None)):
        combo_uri = str(combo_uri_ref)
        if not combo_uri.startswith(COMBO_NS):
            continue
        comment = str(lit)
        for url in re.findall(r"https?://[^\s<>\"]+", comment):
            curie = normalize_identifier(url)
            if curie:
                combo_uri_to_ids[combo_uri].add(curie)
        for cui in re.findall(r"\bC\d{7}\b", comment):
            curie = normalize_identifier(cui)
            if curie:
                combo_uri_to_ids[combo_uri].add(curie)

    for combo_uri in set(combo_uri_to_terms.keys()) | set(combo_uri_to_ids.keys()):
        frag = combo_uri.split("#", 1)[-1]
        frag_term = normalize_text(frag.replace("_", " "))
        if frag_term:
            combo_uri_to_terms[combo_uri].add(frag_term)
            combo_uri_to_native_terms[combo_uri].add(frag_term)

    for pred in (SKOS.exactMatch, SKOS.closeMatch):
        for combo_uri_ref, _, obj in g.triples((None, pred, None)):
            combo_uri = str(combo_uri_ref)
            if not combo_uri.startswith(COMBO_NS):
                continue

            if isinstance(obj, URIRef):
                curie = normalize_identifier(str(obj))
                if curie:
                    combo_uri_to_ids[combo_uri].add(curie)
            elif isinstance(obj, BNode):
                for _, p2, o2 in g.triples((obj, None, None)):
                    if p2 == SKOS.prefLabel:
                        term = normalize_text(str(o2))
                        if term:
                            combo_uri_to_terms[combo_uri].add(term)
                    elif p2 == SKOS.note:
                        note = str(o2).strip()
                        if ":" in note:
                            note_prefix, note_rest = note.split(":", 1)
                            note_curie = normalize_identifier(f"{note_prefix}:{note_rest}")
                            if note_curie:
                                combo_uri_to_ids[combo_uri].add(note_curie)
                        note_id = normalize_identifier(note)
                        if note_id:
                            combo_uri_to_ids[combo_uri].add(note_id)
                        m = re.search(r"/source/([^/]+)/([^/?#]+)$", note)
                        if m:
                            c = source_code_to_curie(m.group(1), m.group(2))
                            if c:
                                combo_uri_to_ids[combo_uri].add(c)

    # Canonicalize alias URIs (e.g. #Cat&apos;s_Claw, #Mindfulness_Based_College)
    # to a preferred numeric COMBO class URI (e.g. #000609, #000528) when there
    # is an unambiguous term-based correspondence.
    all_combo_uris = set(combo_uri_to_terms.keys()) | set(combo_uri_to_ids.keys())
    term_to_numeric_uris: Dict[str, Set[str]] = defaultdict(set)
    label_term_to_numeric_uris: Dict[str, Set[str]] = defaultdict(set)
    for uri in all_combo_uris:
        frag = uri.split("#", 1)[-1]
        if not re.fullmatch(r"\d+", frag):
            continue
        for t in combo_uri_to_terms.get(uri, set()):
            if t:
                term_to_numeric_uris[t].add(uri)
        for t in combo_uri_to_label_terms.get(uri, set()):
            if t:
                label_term_to_numeric_uris[t].add(uri)

    combo_uri_alias_to_canonical: Dict[str, str] = {}
    for uri in all_combo_uris:
        frag = uri.split("#", 1)[-1]
        if re.fullmatch(r"\d+", frag):
            combo_uri_alias_to_canonical[uri] = uri
            continue
        frag_term = normalize_text(frag.replace("_", " "))
        numeric_candidates = term_to_numeric_uris.get(frag_term, set()) if frag_term else set()
        label_candidates = label_term_to_numeric_uris.get(frag_term, set()) if frag_term else set()

        # Prefer exact class-label match over alt-label matches.
        if len(label_candidates) == 1:
            combo_uri_alias_to_canonical[uri] = next(iter(label_candidates))
        elif len(numeric_candidates) == 1:
            combo_uri_alias_to_canonical[uri] = next(iter(numeric_candidates))
        else:
            combo_uri_alias_to_canonical[uri] = uri

    canonical_uri_to_terms: Dict[str, Set[str]] = defaultdict(set)
    canonical_uri_to_native_terms: Dict[str, Set[str]] = defaultdict(set)
    canonical_uri_to_ids: Dict[str, Set[str]] = defaultdict(set)
    canonical_uri_to_locked_umls: Dict[str, Set[str]] = defaultdict(set)

    for uri, terms in combo_uri_to_terms.items():
        canonical = combo_uri_alias_to_canonical.get(uri, uri)
        canonical_uri_to_terms[canonical].update(terms)
    for uri, terms in combo_uri_to_native_terms.items():
        canonical = combo_uri_alias_to_canonical.get(uri, uri)
        canonical_uri_to_native_terms[canonical].update(terms)
    for uri, ids in combo_uri_to_ids.items():
        canonical = combo_uri_alias_to_canonical.get(uri, uri)
        canonical_uri_to_ids[canonical].update(ids)
    for uri, locked in combo_uri_to_locked_umls.items():
        canonical = combo_uri_alias_to_canonical.get(uri, uri)
        canonical_uri_to_locked_umls[canonical].update(locked)

    combo_uri_to_terms = canonical_uri_to_terms
    combo_uri_to_native_terms = canonical_uri_to_native_terms
    combo_uri_to_ids = canonical_uri_to_ids
    combo_uri_to_locked_umls = canonical_uri_to_locked_umls

    combo_term_to_uris: Dict[str, Set[str]] = defaultdict(set)
    combo_native_term_to_uris: Dict[str, Set[str]] = defaultdict(set)
    combo_id_to_uris: Dict[str, Set[str]] = defaultdict(set)

    for uri, terms in combo_uri_to_terms.items():
        for term in terms:
            combo_term_to_uris[term].add(uri)
    for uri, terms in combo_uri_to_native_terms.items():
        for term in terms:
            combo_native_term_to_uris[term].add(uri)

    for uri, ids in combo_uri_to_ids.items():
        for curie in ids:
            combo_id_to_uris[curie].add(uri)

    return (
        combo_uri_to_terms,
        combo_uri_to_native_terms,
        combo_uri_to_ids,
        combo_term_to_uris,
        combo_native_term_to_uris,
        combo_id_to_uris,
        combo_uri_to_locked_umls,
        combo_uri_alias_to_canonical,
    )


def build_embedding_index_maps(
    semmed_concepts_path: Path,
    ikraph_names_flat_path: Path,
) -> Tuple[List[str], List[str]]:
    semmed_index_to_key: List[str] = []
    ikraph_index_to_key: List[str] = []

    if semmed_concepts_path.exists():
        with semmed_concepts_path.open(newline="", encoding="utf-8", errors="replace") as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row:
                    semmed_index_to_key.append("")
                    continue
                raw = (row[0] or "").strip()
                if re.fullmatch(r"C\d{7}", raw):
                    semmed_index_to_key.append(f"UMLS:{raw}")
                else:
                    semmed_index_to_key.append(raw)

    if ikraph_names_flat_path.exists():
        with ikraph_names_flat_path.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                parts = line.rstrip("\n").split("\t")
                ikraph_index_to_key.append(parts[0].strip() if parts else "")

    return semmed_index_to_key, ikraph_index_to_key


def load_embedding_best_matches(
    path: Path,
    semmed_index_to_key: List[str],
    ikraph_index_to_key: List[str],
    combo_uri_alias_to_canonical: Dict[str, str],
) -> Dict[Tuple[str, str, str], Tuple[float, str]]:
    """
    Load best embedding matches keyed by:
      (dataset, combo_uri, source_key) -> (score, external_name)
    """
    if not path.exists():
        return {}

    out: Dict[Tuple[str, str, str], Tuple[float, str]] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            source = (row.get("source") or "").strip().upper()
            if source == "SEMMED":
                dataset = "semmed"
                index_map = semmed_index_to_key
            elif source == "IKRAPH":
                dataset = "ikraph"
                index_map = ikraph_index_to_key
            else:
                continue

            combini_id = (row.get("combini_id") or "").strip()
            combini_label = (row.get("combini_label") or "").strip()
            ext_name_raw = (row.get("ext_name") or "").strip()
            ext_index_raw = (row.get("ext_index") or "").strip()
            if not ext_name_raw or not ext_index_raw:
                continue

            score_raw = (row.get("score") or "").strip()
            try:
                score = float(score_raw)
            except ValueError:
                continue

            try:
                ext_index = int(ext_index_raw)
            except ValueError:
                continue
            if ext_index < 0 or ext_index >= len(index_map):
                continue
            source_key = (index_map[ext_index] or "").strip()
            if not source_key or source_key.upper() in {"NA", "N/A"}:
                continue

            candidate_uris = []
            if combini_id:
                candidate_uris.append(f"{COMBO_NS}{combini_id}")
            if combini_label:
                candidate_uris.append(f"{COMBO_NS}{combini_label.replace(' ', '_')}")

            for combo_uri in candidate_uris:
                combo_uri = combo_uri_alias_to_canonical.get(combo_uri, combo_uri)
                key = (dataset, combo_uri, source_key)
                prev = out.get(key)
                if prev is None or score > prev[0]:
                    out[key] = (score, ext_name_raw)
    return out


def load_denylist(path: Path) -> Set[Tuple[str, str, str]]:
    """
    Load hard-deny mappings keyed by (dataset, source_key, combo_uri).
    Rows with missing required fields are ignored.
    """
    denied: Set[Tuple[str, str, str]] = set()
    if not path.exists():
        return denied

    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        required = {"dataset", "source_key", "combo_uri"}
        if not required.issubset(set(reader.fieldnames or [])):
            return denied
        for row in reader:
            dataset = (row.get("dataset") or "").strip().lower()
            source_key = (row.get("source_key") or "").strip()
            combo_uri = (row.get("combo_uri") or "").strip()
            if not dataset or not source_key or not combo_uri:
                continue
            denied.add((dataset, source_key, combo_uri))
    return denied


def load_semmed_labels(concept_csv: Path) -> Dict[str, Set[str]]:
    labels: Dict[str, Set[str]] = defaultdict(set)
    with concept_csv.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if len(row) < 5:
                continue
            cui = (row[0] or "").strip()
            if not cui:
                continue
            for candidate in (row[1], row[4]):
                n = normalize_text(candidate)
                if n:
                    labels[cui].add(n)
    return labels


def load_ikraph_lookup(ikraph_json: Path):
    by_biokdeid: Dict[str, Dict[str, object]] = {}
    with ikraph_json.open("rb") as fh:
        for item in ijson.items(fh, "item"):
            if not isinstance(item, dict):
                continue
            biokdeid = str(item.get("biokdeid", "")).strip()
            if not biokdeid:
                continue
            raw_id = str(item.get("id", "")).strip()
            labels = set()
            for field in ("official name", "common name"):
                n = normalize_text(str(item.get(field, "")))
                if n:
                    labels.add(n)
            by_biokdeid[biokdeid] = {
                "id": raw_id,
                "labels": labels,
                "type": str(item.get("type", "")),
            }
    return by_biokdeid


def match_normalized_nodes(
    source_name: str,
    normalized_json: Path,
    combo_term_to_uris: Dict[str, Set[str]],
    combo_native_term_to_uris: Dict[str, Set[str]],
    combo_id_to_uris: Dict[str, Set[str]],
    combo_uri_to_locked_umls: Dict[str, Set[str]],
    embedding_index: Dict[Tuple[str, str, str], Tuple[float, str]],
    denylist: Set[Tuple[str, str, str]],
    semmed_labels: Dict[str, Set[str]],
):
    matched_nodes: List[Dict[str, object]] = []

    with normalized_json.open("rb") as fh:
        for key, value in ijson.kvitems(fh, ""):
            identifiers: Set[str] = set()
            terms: Set[str] = set()

            key_norm = normalize_identifier(str(key))
            if key_norm:
                identifiers.add(key_norm)

            if source_name == "semmed":
                raw_key = str(key).strip()
                if re.fullmatch(r"C\d{7}", raw_key):
                    for t in semmed_labels.get(raw_key, set()):
                        terms.add(t)
                if raw_key.startswith("UMLS:"):
                    c = raw_key.split(":", 1)[-1]
                    for t in semmed_labels.get(c, set()):
                        terms.add(t)

            if isinstance(value, dict):
                node_id = value.get("id") or {}
                if isinstance(node_id, dict):
                    identifier = normalize_identifier(str(node_id.get("identifier", "")))
                    if identifier:
                        identifiers.add(identifier)
                    label = normalize_text(str(node_id.get("label", "")))
                    if label:
                        terms.add(label)

                eqs = value.get("equivalent_identifiers") or []
                if isinstance(eqs, list):
                    for eq in eqs:
                        if not isinstance(eq, dict):
                            continue
                        identifier = normalize_identifier(str(eq.get("identifier", "")))
                        if identifier:
                            identifiers.add(identifier)
                        label = normalize_text(str(eq.get("label", "")))
                        if label:
                            terms.add(label)

            matched_uris: Set[str] = set()
            matched_ids: Set[str] = set()
            matched_terms: Set[str] = set()
            id_supported_uris: Set[str] = set()

            for ident in identifiers:
                uris = combo_id_to_uris.get(ident)
                if uris:
                    matched_uris.update(uris)
                    id_supported_uris.update(uris)
                    matched_ids.add(ident)

            for term in terms:
                uris = combo_term_to_uris.get(term)
                if uris:
                    # Guardrail: short single-token strings (e.g., "smt", "msc", "cam")
                    # are too ambiguous alone; only keep if identifiers already
                    # matched this node, and then only within those ID-backed URIs.
                    if is_ambiguous_short_token(term):
                        if not id_supported_uris:
                            continue
                        uris = set(uris).intersection(id_supported_uris)
                        if not uris:
                            continue

                    native_uris = combo_native_term_to_uris.get(term, set())
                    filtered_by_support: Set[str] = set()
                    for uri in uris:
                        if uri in native_uris:
                            filtered_by_support.add(uri)
                            continue

                        # Non-native terms (often coming from enrichment bnodes) need
                        # stronger support: either identifier anchor to same URI, or
                        # a strong embedding hit for this dataset/source_key/URI.
                        if uri in id_supported_uris:
                            filtered_by_support.add(uri)
                            continue
                        emb = embedding_index.get((source_name, uri, str(key)))
                        if emb is not None and emb[0] >= NON_NATIVE_TERM_EMBEDDING_MIN:
                            filtered_by_support.add(uri)

                    uris = filtered_by_support
                    if not uris:
                        continue
                    matched_uris.update(uris)
                    matched_terms.add(term)

            if matched_uris:
                matched_uris = {
                    u for u in matched_uris if (source_name, str(key), u) not in denylist
                }
            if matched_uris:
                matched_nodes.append(
                    {
                        "dataset": source_name,
                        "source_key": str(key),
                        "identifiers": sorted(identifiers),
                        "terms": sorted(terms),
                        "matched_combo_uris": sorted(matched_uris),
                        "matched_by_identifiers": sorted(matched_ids),
                        "matched_by_terms": sorted(matched_terms),
                    }
                )

    return matched_nodes


def apply_umls_lock_to_matched_nodes(
    matched_nodes: List[Dict[str, object]],
    source_name: str,
    combo_uri_to_locked_umls: Dict[str, Set[str]],
) -> List[Dict[str, object]]:
    filtered_nodes: List[Dict[str, object]] = []
    for node in matched_nodes:
        if str(node.get("dataset")) != source_name:
            filtered_nodes.append(node)
            continue

        identifiers = {str(x) for x in node.get("identifiers", []) if str(x)}
        key_norm = normalize_identifier(str(node.get("source_key", "")))
        allowed_uris: Set[str] = set()

        for uri in [str(u) for u in node.get("matched_combo_uris", []) if str(u)]:
            locked = combo_uri_to_locked_umls.get(uri, set())
            if not locked:
                allowed_uris.add(uri)
                continue

            if source_name == "semmed":
                # Strict SemMed lock against source_key CUI.
                if key_norm and key_norm in locked:
                    allowed_uris.add(uri)
            else:
                # iKraph (and others): require explicit identifier evidence.
                if identifiers.intersection(locked):
                    allowed_uris.add(uri)

        if not allowed_uris:
            continue

        node["matched_combo_uris"] = sorted(allowed_uris)
        filtered_nodes.append(node)
    return filtered_nodes


def augment_with_embedding_matches(
    matched_nodes: List[Dict[str, object]],
    dataset: str,
    embedding_index: Dict[Tuple[str, str, str], Tuple[float, str]],
    combo_uri_to_locked_umls: Dict[str, Set[str]],
    denylist: Set[Tuple[str, str, str]],
):
    by_source_key: Dict[str, Dict[str, object]] = {
        str(node["source_key"]): node for node in matched_nodes if str(node.get("dataset")) == dataset
    }

    for (emb_dataset, combo_uri, source_key), (_score, ext_name) in embedding_index.items():
        if emb_dataset != dataset:
            continue
        if (dataset, source_key, combo_uri) in denylist:
            continue
        if dataset == "semmed":
            locked = combo_uri_to_locked_umls.get(combo_uri, set())
            norm_source = normalize_identifier(source_key)
            if locked and (not norm_source or norm_source not in locked):
                continue

        node = by_source_key.get(source_key)
        norm_ident = normalize_identifier(source_key)
        norm_term = normalize_text(ext_name)
        if node is None:
            matched_terms = set()
            if norm_term:
                matched_terms.add(norm_term)

            node = {
                "dataset": dataset,
                "source_key": source_key,
                "identifiers": sorted({norm_ident} if norm_ident else set()),
                "terms": sorted({norm_term} if norm_term else set()),
                "matched_combo_uris": [combo_uri],
                "matched_by_identifiers": [],
                "matched_by_terms": sorted(matched_terms),
            }
            matched_nodes.append(node)
            by_source_key[source_key] = node
            continue

        combo_uris = set(str(x) for x in node.get("matched_combo_uris", []))
        combo_uris.add(combo_uri)
        node["matched_combo_uris"] = sorted(combo_uris)

        identifiers = set(str(x) for x in node.get("identifiers", []))
        if norm_ident:
            identifiers.add(norm_ident)
        node["identifiers"] = sorted(identifiers)

        if norm_term:
            terms = set(str(x) for x in node.get("terms", []))
            terms.add(norm_term)
            node["terms"] = sorted(terms)

            matched_terms = set(str(x) for x in node.get("matched_by_terms", []))
            matched_terms.add(norm_term)
            node["matched_by_terms"] = sorted(matched_terms)


def build_cross_normalization_links(matched_nodes: List[Dict[str, object]]):
    term_groups: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for node in matched_nodes:
        for t in node.get("matched_by_terms", []):
            term_groups[str(t)].append(node)

    links = []
    for term, group in term_groups.items():
        if len(group) < 2:
            continue
        seen_pairs: Set[Tuple[str, str, str, str]] = set()
        for i in range(len(group)):
            left = group[i]
            for j in range(i + 1, len(group)):
                right = group[j]
                if left["dataset"] == right["dataset"]:
                    continue
                k = (
                    str(left["dataset"]),
                    str(left["source_key"]),
                    str(right["dataset"]),
                    str(right["source_key"]),
                )
                if k in seen_pairs:
                    continue
                seen_pairs.add(k)
                links.append(
                    {
                        "normalization_term": term,
                        "left_dataset": left["dataset"],
                        "left_source_key": left["source_key"],
                        "right_dataset": right["dataset"],
                        "right_source_key": right["source_key"],
                    }
                )
    return links


def extract_semmed_edges(
    edge_csv: Path,
    matched_semmed_cuis: Set[str],
) -> Tuple[List[List[str]], List[List[str]]]:
    adjacent: List[List[str]] = []
    induced: List[List[str]] = []

    with edge_csv.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header:
            adjacent.append(header)
            induced.append(header)

        for row in reader:
            if len(row) < 2:
                continue
            start = row[0].strip()
            end = row[1].strip()
            start_hit = start in matched_semmed_cuis
            end_hit = end in matched_semmed_cuis

            if start_hit or end_hit:
                adjacent.append(row)
            if start_hit and end_hit:
                induced.append(row)

    return adjacent, induced


def extract_ikraph_edges(
    edge_csv: Path,
    matched_ikraph_identifiers: Set[str],
    ikraph_lookup: Dict[str, Dict[str, object]],
) -> Tuple[List[List[str]], List[List[str]]]:
    adjacent: List[List[str]] = []
    induced: List[List[str]] = []

    with edge_csv.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header:
            adjacent.append(header)
            induced.append(header)

        for row in reader:
            if len(row) < 3:
                continue
            n1 = row[1].strip()
            n2 = row[2].strip()

            n1_norm = None
            n2_norm = None
            if n1 in ikraph_lookup:
                n1_norm = normalize_identifier(str(ikraph_lookup[n1].get("id", "")))
            if n2 in ikraph_lookup:
                n2_norm = normalize_identifier(str(ikraph_lookup[n2].get("id", "")))

            n1_hit = (n1_norm in matched_ikraph_identifiers) if n1_norm else False
            n2_hit = (n2_norm in matched_ikraph_identifiers) if n2_norm else False

            if n1_hit or n2_hit:
                adjacent.append(row)
            if n1_hit and n2_hit:
                induced.append(row)

    return adjacent, induced


def write_tsv(path: Path, rows: Iterable[Iterable[object]]):
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter="\t")
        for row in rows:
            writer.writerow(list(row))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract COMBO-focused subgraph from normalized SemMedDB/iKraph.")
    parser.add_argument(
        "--semmed-normalized",
        default="/Users/drshika2/NodeNormalization/semmed_ikraph_normalized/semmed_normalized_full_with_pubchem.json",
    )
    parser.add_argument(
        "--ikraph-normalized",
        default="/Users/drshika2/NodeNormalization/semmed_ikraph_normalized/ikraph_normalized_full.with_newly_normalized_20260123_v6_20260218.json",
    )
    parser.add_argument(
        "--semmed-concepts",
        default="/Users/drshika2/neo4jexploration/semmed_data/concept.csv",
    )
    parser.add_argument(
        "--ikraph-nodes",
        default="/Users/drshika2/neo4jexploration/iKraph_raw/iKraph_full/NER_ID_dict_cap_final.json",
    )
    parser.add_argument(
        "--semmed-edges",
        default="/Users/drshika2/NodeNormalization/semmed_ikraph_normalized/semmeddb_edges_cleaned.csv",
    )
    parser.add_argument(
        "--ikraph-edges",
        default="/Users/drshika2/NodeNormalization/semmed_ikraph_normalized/ikraph_edges_cleaned.csv",
    )
    parser.add_argument(
        "--combo-owl",
        default="/Users/drshika2/NodeNormalization/COMBO_20260115_with_skos.owl",
    )
    parser.add_argument(
        "--output-dir",
        default="/Users/drshika2/NodeNormalization/semmed_ikraph_normalized/combo_subgraph",
    )
    parser.add_argument(
        "--embedding-best",
        default="/Users/drshika2/NodeNormalization/combo_external_highsim_bestper_source.tsv",
    )
    parser.add_argument(
        "--ikraph-names-flat",
        default="/Users/drshika2/NodeNormalization/ikraph_names_flat.tsv",
    )
    parser.add_argument(
        "--denylist-file",
        default="/Users/drshika2/NodeNormalization/curated_false_matches.tsv",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    semmed_normalized = Path(args.semmed_normalized)
    ikraph_normalized = Path(args.ikraph_normalized)
    semmed_concepts = Path(args.semmed_concepts)
    ikraph_nodes = Path(args.ikraph_nodes)
    semmed_edges = Path(args.semmed_edges)
    ikraph_edges = Path(args.ikraph_edges)
    combo_owl = Path(args.combo_owl)
    output_dir = Path(args.output_dir)
    embedding_best = Path(args.embedding_best)
    ikraph_names_flat = Path(args.ikraph_names_flat)
    denylist_file = Path(args.denylist_file)
    output_dir.mkdir(parents=True, exist_ok=True)

    (
        combo_uri_to_terms,
        combo_uri_to_native_terms,
        combo_uri_to_ids,
        combo_term_to_uris,
        combo_native_term_to_uris,
        combo_id_to_uris,
        combo_uri_to_locked_umls,
        combo_uri_alias_to_canonical,
    ) = parse_combo_ontology(combo_owl)
    semmed_index_to_key, ikraph_index_to_key = build_embedding_index_maps(
        semmed_concepts_path=semmed_concepts,
        ikraph_names_flat_path=ikraph_names_flat,
    )
    embedding_index = load_embedding_best_matches(
        path=embedding_best,
        semmed_index_to_key=semmed_index_to_key,
        ikraph_index_to_key=ikraph_index_to_key,
        combo_uri_alias_to_canonical=combo_uri_alias_to_canonical,
    )
    denylist = load_denylist(denylist_file)

    semmed_label_lookup = load_semmed_labels(semmed_concepts)
    ikraph_lookup = load_ikraph_lookup(ikraph_nodes)

    semmed_matched = match_normalized_nodes(
        source_name="semmed",
        normalized_json=semmed_normalized,
        combo_term_to_uris=combo_term_to_uris,
        combo_native_term_to_uris=combo_native_term_to_uris,
        combo_id_to_uris=combo_id_to_uris,
        combo_uri_to_locked_umls=combo_uri_to_locked_umls,
        embedding_index=embedding_index,
        denylist=denylist,
        semmed_labels=semmed_label_lookup,
    )
    ikraph_matched = match_normalized_nodes(
        source_name="ikraph",
        normalized_json=ikraph_normalized,
        combo_term_to_uris=combo_term_to_uris,
        combo_native_term_to_uris=combo_native_term_to_uris,
        combo_id_to_uris=combo_id_to_uris,
        combo_uri_to_locked_umls=combo_uri_to_locked_umls,
        embedding_index=embedding_index,
        denylist=denylist,
        semmed_labels={},
    )

    # Inject embedding-derived matches (resolved to real source keys) so they are
    # represented as seed mappings with explicit confidence/provenance downstream.
    augment_with_embedding_matches(
        matched_nodes=semmed_matched,
        dataset="semmed",
        embedding_index=embedding_index,
        combo_uri_to_locked_umls=combo_uri_to_locked_umls,
        denylist=denylist,
    )
    augment_with_embedding_matches(
        matched_nodes=ikraph_matched,
        dataset="ikraph",
        embedding_index=embedding_index,
        combo_uri_to_locked_umls=combo_uri_to_locked_umls,
        denylist=denylist,
    )

    semmed_matched = apply_umls_lock_to_matched_nodes(
        matched_nodes=semmed_matched,
        source_name="semmed",
        combo_uri_to_locked_umls=combo_uri_to_locked_umls,
    )
    ikraph_matched = apply_umls_lock_to_matched_nodes(
        matched_nodes=ikraph_matched,
        source_name="ikraph",
        combo_uri_to_locked_umls=combo_uri_to_locked_umls,
    )

    all_matched = semmed_matched + ikraph_matched
    cross_links = build_cross_normalization_links(all_matched)

    matched_semmed_cuis: Set[str] = set()
    for node in semmed_matched:
        for ident in node["identifiers"]:
            if str(ident).startswith("UMLS:"):
                matched_semmed_cuis.add(str(ident).split(":", 1)[1])

    matched_ikraph_identifiers: Set[str] = set()
    for node in ikraph_matched:
        for ident in node["identifiers"]:
            matched_ikraph_identifiers.add(str(ident))

    semmed_adjacent, semmed_induced = extract_semmed_edges(semmed_edges, matched_semmed_cuis)
    ikraph_adjacent, ikraph_induced = extract_ikraph_edges(
        ikraph_edges,
        matched_ikraph_identifiers,
        ikraph_lookup,
    )

    seed_rows = [
        [
            "dataset",
            "source_key",
            "matched_combo_uris",
            "matched_by_identifiers",
            "matched_by_terms",
            "matched_by_embeddings",
            "embedding_confidence",
            "identifiers",
            "terms",
        ]
    ]
    embedding_link_rows = [
        [
            "dataset",
            "source_key",
            "combo_uri",
            "embedding_term",
            "embedding_confidence",
            "mapping_source",
        ]
    ]
    for node in all_matched:
        embedding_hits: List[Tuple[str, str, float]] = []
        dataset = str(node["dataset"])
        source_key = str(node["source_key"])
        for combo_uri in node["matched_combo_uris"]:
            key = (dataset, str(combo_uri), source_key)
            embedding = embedding_index.get(key)
            if embedding is not None:
                score, ext_name = embedding
                embedding_hits.append((str(combo_uri), ext_name, score))

        matched_by_embeddings = sorted({f"{uri}|{term}" for uri, term, _ in embedding_hits})
        embedding_confidence = max((score for _, _, score in embedding_hits), default=None)

        seed_rows.append(
            [
                node["dataset"],
                node["source_key"],
                "|".join(node["matched_combo_uris"]),
                "|".join(node["matched_by_identifiers"]),
                "|".join(node["matched_by_terms"]),
                "|".join(matched_by_embeddings),
                f"{embedding_confidence:.4f}" if embedding_confidence is not None else "",
                "|".join(node["identifiers"]),
                "|".join(node["terms"]),
            ]
        )
        for combo_uri, term, score in embedding_hits:
            embedding_link_rows.append(
                [
                    node["dataset"],
                    node["source_key"],
                    combo_uri,
                    term,
                    f"{score:.4f}",
                    "embedding",
                ]
            )

    norm_rows = [["normalization_term", "left_dataset", "left_source_key", "right_dataset", "right_source_key"]]
    for link in cross_links:
        norm_rows.append(
            [
                link["normalization_term"],
                link["left_dataset"],
                link["left_source_key"],
                link["right_dataset"],
                link["right_source_key"],
            ]
        )

    write_tsv(output_dir / "combo_seed_nodes.tsv", seed_rows)
    write_tsv(output_dir / "combo_embedding_links.tsv", embedding_link_rows)
    write_tsv(output_dir / "combo_normalization_links.tsv", norm_rows)
    write_tsv(output_dir / "semmed_edges_seed_adjacent.tsv", semmed_adjacent)
    write_tsv(output_dir / "semmed_edges_seed_induced.tsv", semmed_induced)
    write_tsv(output_dir / "ikraph_edges_seed_adjacent.tsv", ikraph_adjacent)
    write_tsv(output_dir / "ikraph_edges_seed_induced.tsv", ikraph_induced)

    summary = {
        "combo_concepts_with_terms": len(combo_uri_to_terms),
        "combo_concepts_with_ids": len(combo_uri_to_ids),
        "combo_unique_terms": len(combo_term_to_uris),
        "combo_unique_ids": len(combo_id_to_uris),
        "semmed_seed_nodes": len(semmed_matched),
        "ikraph_seed_nodes": len(ikraph_matched),
        "seed_nodes_total": len(all_matched),
        "cross_dataset_normalization_links": len(cross_links),
        "semmed_edges_seed_adjacent": max(len(semmed_adjacent) - 1, 0),
        "semmed_edges_seed_induced": max(len(semmed_induced) - 1, 0),
        "ikraph_edges_seed_adjacent": max(len(ikraph_adjacent) - 1, 0),
        "ikraph_edges_seed_induced": max(len(ikraph_induced) - 1, 0),
        "embedding_link_rows": max(len(embedding_link_rows) - 1, 0),
    }
    with (output_dir / "combo_subgraph_summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    print("Wrote COMBO subgraph outputs to", output_dir)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
