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
import itertools
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


def is_negative_predicate(predicate: str) -> bool:
    p = (predicate or "").strip().lower()
    if not p:
        return False
    if p.startswith("neg_"):
        return True
    if "negative" in p:
        return True
    return False


def semmed_cuis_from_edge_endpoint(value: str) -> Set[str]:
    """
    Parse SemMed edge endpoints into one-or-more candidate CUIs.
    Supports pipe-delimited alternatives but only keeps explicit CUI tokens.

    Important:
    - We intentionally do NOT coerce bare numeric fragments (e.g. `1302`) to
      CUIs, because values like `K142|1302` in SemMed can represent composite
      gene/protein identifiers rather than UMLS concepts.
    """
    raw = (value or "").strip()
    if not raw:
        return set()

    out: Set[str] = set()
    for token in raw.split("|"):
        tok = token.strip()
        if not tok:
            continue

        if re.fullmatch(r"C\d{7}", tok):
            out.add(tok)
            continue

        if re.fullmatch(r"C\d{6}", tok):
            out.add(tok)
            out.add(f"C{tok[1:].zfill(7)}")
            continue

    return out


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
    allow_embedding_support: bool,
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
                        emb = embedding_index.get((source_name, uri, str(key))) if allow_embedding_support else None
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
    # Emit a stable header regardless of source file format.
    adjacent: List[List[str]] = [[":START_ID", ":END_ID", ":TYPE", "frequency%"]]
    induced: List[List[str]] = [[":START_ID", ":END_ID", ":TYPE", "frequency%"]]

    with edge_csv.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        first_row = next(reader, None)
        if first_row is None:
            return adjacent, induced

        # Source files can be headerless; detect common header patterns.
        first0 = (first_row[0] if len(first_row) > 0 else "").strip().lower()
        first1 = (first_row[1] if len(first_row) > 1 else "").strip().lower()
        first2 = (first_row[2] if len(first_row) > 2 else "").strip().lower()
        has_header = (
            first0 in {":start_id", "start_id", "subject"} or
            first1 in {":end_id", "end_id", "object"} or
            first2 in {":type", "type", "predicate"}
        )
        rows_iter = reader if has_header else itertools.chain([first_row], reader)

        for row in rows_iter:
            if len(row) < 3:
                continue
            start_cuis = semmed_cuis_from_edge_endpoint(row[0])
            end_cuis = semmed_cuis_from_edge_endpoint(row[1])
            pred = row[2].strip()
            freq = row[3].strip() if len(row) > 3 else "0"

            # Keep only concept-to-concept SemMed edges.
            if not start_cuis or not end_cuis:
                continue
            # Drop self loops.
            if start_cuis.intersection(end_cuis):
                continue
            # Drop negated predicates.
            if is_negative_predicate(pred):
                continue

            start_hit = bool(start_cuis.intersection(matched_semmed_cuis))
            end_hit = bool(end_cuis.intersection(matched_semmed_cuis))

            if not (start_hit or end_hit):
                continue

            # Emit normalized CUI endpoints so edge nodes share ID namespace
            # with SemMed seed nodes (UMLS CUI-based source keys).
            for s_cui in sorted(start_cuis):
                for e_cui in sorted(end_cuis):
                    if s_cui == e_cui:
                        continue
                    out_row = [s_cui, e_cui, pred, freq]
                    adjacent.append(out_row)
                    if start_hit and end_hit:
                        induced.append(out_row)

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
            pred_label = row[16].strip() if len(row) > 16 else ""

            n1_norm = None
            n2_norm = None
            if n1 in ikraph_lookup:
                n1_norm = normalize_identifier(str(ikraph_lookup[n1].get("id", "")))
            if n2 in ikraph_lookup:
                n2_norm = normalize_identifier(str(ikraph_lookup[n2].get("id", "")))

            # Keep only concept-to-concept edges (both endpoints must resolve).
            if not n1_norm or not n2_norm:
                continue
            # Drop self loops.
            if n1_norm == n2_norm:
                continue
            # Drop negative edges from iKraph labels.
            if is_negative_predicate(pred_label):
                continue

            n1_hit = n1_norm in matched_ikraph_identifiers
            n2_hit = n2_norm in matched_ikraph_identifiers

            if n1_hit or n2_hit:
                out_row = list(row)
                # Normalize endpoint IDs to the same namespace as iKraph seeds.
                out_row[1] = n1_norm
                out_row[2] = n2_norm
                adjacent.append(out_row)
                if n1_hit and n2_hit:
                    induced.append(out_row)

    return adjacent, induced


def build_combo_intervention_subtree_from_owl(owl_path: Path, root_fragment: str) -> Set[str]:
    g = Graph()
    g.parse(str(owl_path))

    combo_uris: Set[str] = set()
    children_by_parent: Dict[str, Set[str]] = defaultdict(set)
    labels_by_uri: Dict[str, Set[str]] = defaultdict(set)
    normalized_target = normalize_text(root_fragment.replace("_", " "))

    for s, _, o in g.triples((None, RDFS.subClassOf, None)):
        su = str(s)
        ou = str(o)
        if su.startswith(COMBO_NS):
            combo_uris.add(su)
        if ou.startswith(COMBO_NS):
            combo_uris.add(ou)
        if su.startswith(COMBO_NS) and ou.startswith(COMBO_NS):
            children_by_parent[ou].add(su)

    for s, _, lit in g.triples((None, RDFS.label, None)):
        su = str(s)
        if su.startswith(COMBO_NS):
            combo_uris.add(su)
            lbl = normalize_text(str(lit))
            if lbl:
                labels_by_uri[su].add(lbl)

    for s, _, lit in g.triples((None, SKOS.prefLabel, None)):
        su = str(s)
        if su.startswith(COMBO_NS):
            combo_uris.add(su)
            lbl = normalize_text(str(lit))
            if lbl:
                labels_by_uri[su].add(lbl)

    root_candidates = [
        u for u in combo_uris
        if u.split("#", 1)[-1] == root_fragment
    ]
    if not root_candidates:
        root_candidates = [
            u for u in combo_uris
            if normalize_text(u.split("#", 1)[-1].replace("_", " ")) == normalized_target
        ]
    if not root_candidates:
        root_candidates = [
            u for u in combo_uris
            if normalized_target in labels_by_uri.get(u, set())
        ]
    if not root_candidates:
        return set()

    root_uri = sorted(root_candidates)[0]
    visited: Set[str] = set()
    queue: List[str] = [root_uri]
    while queue:
        parent = queue.pop(0)
        if parent in visited:
            continue
        visited.add(parent)
        for child in sorted(children_by_parent.get(parent, set())):
            if child not in visited:
                queue.append(child)
    return visited


def select_intervention_umls_only_combo_uris(
    intervention_combo_uris: Set[str],
    combo_uri_to_ids: Dict[str, Set[str]],
) -> Tuple[Set[str], Set[str]]:
    eligible_combo_uris: Set[str] = set()
    eligible_umls_ids: Set[str] = set()
    for uri in intervention_combo_uris:
        ids = {x for x in combo_uri_to_ids.get(uri, set()) if x}
        if not ids:
            continue
        if all(x.startswith("UMLS:") for x in ids):
            cui_ids = {x for x in ids if re.fullmatch(r"UMLS:C\d+", x)}
            if not cui_ids:
                continue
            eligible_combo_uris.add(uri)
            eligible_umls_ids.update(cui_ids)
    return eligible_combo_uris, eligible_umls_ids


def filter_matched_nodes_for_combo_uris_and_umls(
    matched_nodes: List[Dict[str, object]],
    allowed_combo_uris: Set[str],
    allowed_umls_ids: Set[str],
) -> Tuple[List[Dict[str, object]], Dict[Tuple[str, str], List[str]]]:
    filtered: List[Dict[str, object]] = []
    node_umls_hits: Dict[Tuple[str, str], List[str]] = {}

    for node in matched_nodes:
        dataset = str(node.get("dataset") or "")
        source_key = str(node.get("source_key") or "")
        matched_combo_uris = [str(u) for u in node.get("matched_combo_uris", []) if str(u)]
        combo_hits = sorted({u for u in matched_combo_uris if u in allowed_combo_uris})
        if not combo_hits:
            continue

        ids = {normalize_identifier(source_key)} if source_key else set()
        ids.update(normalize_identifier(str(x)) for x in node.get("identifiers", []))
        ids = {x for x in ids if x}
        umls_hits = sorted(x for x in ids if x in allowed_umls_ids)
        if not umls_hits:
            continue

        keep = dict(node)
        keep["matched_combo_uris"] = combo_hits
        filtered.append(keep)
        node_umls_hits[(dataset, source_key)] = umls_hits

    return filtered, node_umls_hits


def label_for_dataset_presence(has_semmed: bool, has_ikraph: bool) -> str:
    if has_semmed and has_ikraph:
        return "CM Intervention Both"
    if has_ikraph:
        return "CM Intervention iKraph"
    return "CM Intervention SemMed"


def build_cm_intervention_presence_rows(
    semmed_nodes: List[Dict[str, object]],
    ikraph_nodes: List[Dict[str, object]],
    node_umls_hits: Dict[Tuple[str, str], List[str]],
) -> Tuple[List[List[object]], Dict[str, str]]:
    datasets_by_umls: Dict[str, Set[str]] = defaultdict(set)
    semmed_seed_count: Dict[str, int] = defaultdict(int)
    ikraph_seed_count: Dict[str, int] = defaultdict(int)

    for node in semmed_nodes:
        key = (str(node.get("dataset") or ""), str(node.get("source_key") or ""))
        for umls_id in node_umls_hits.get(key, []):
            datasets_by_umls[umls_id].add("semmed")
            semmed_seed_count[umls_id] += 1
    for node in ikraph_nodes:
        key = (str(node.get("dataset") or ""), str(node.get("source_key") or ""))
        for umls_id in node_umls_hits.get(key, []):
            datasets_by_umls[umls_id].add("ikraph")
            ikraph_seed_count[umls_id] += 1

    header = [
        "umls_id",
        "cm_intervention_presence_label",
        "present_in_semmed",
        "present_in_ikraph",
        "semmed_seed_count",
        "ikraph_seed_count",
    ]
    rows: List[List[object]] = [header]
    label_by_umls: Dict[str, str] = {}
    for umls_id in sorted(datasets_by_umls.keys()):
        has_semmed = "semmed" in datasets_by_umls[umls_id]
        has_ikraph = "ikraph" in datasets_by_umls[umls_id]
        label = label_for_dataset_presence(has_semmed=has_semmed, has_ikraph=has_ikraph)
        label_by_umls[umls_id] = label
        rows.append(
            [
                umls_id,
                label,
                "true" if has_semmed else "false",
                "true" if has_ikraph else "false",
                semmed_seed_count.get(umls_id, 0),
                ikraph_seed_count.get(umls_id, 0),
            ]
        )
    return rows, label_by_umls


def build_fallback_semmed_metadata(concept_csv_path: Path, target_cuis: Set[str]) -> Dict[str, dict]:
    fallback: Dict[str, dict] = {}
    if not concept_csv_path.exists() or not target_cuis:
        return fallback
    with concept_csv_path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if len(row) < 3:
                continue
            cui = (row[0] or "").strip()
            if cui not in target_cuis or cui in fallback:
                continue
            name = (row[1] or "").strip()
            semtype = (row[2] or "").strip()
            text = (row[4] or "").strip() if len(row) > 4 else ""
            display_name = name or text or f"UMLS:{cui}"
            primary_type = f"semmed:{semtype}" if semtype else ""
            fallback[cui] = {
                "display_name": display_name,
                "primary_type": primary_type,
                "types": primary_type,
            }
    return fallback


def load_semmed_metadata_for_cuis(
    semmed_normalized_path: Path,
    semmed_concept_csv_path: Path,
    target_cuis: Set[str],
) -> Dict[str, dict]:
    if not target_cuis:
        return {}

    target_curies = {f"UMLS:{cui}" for cui in target_cuis}
    md_by_cui: Dict[str, dict] = {}
    with semmed_normalized_path.open("rb") as fh:
        for curie, entry in ijson.kvitems(fh, ""):
            curie_s = str(curie).strip()
            if curie_s not in target_curies or not isinstance(entry, dict):
                continue
            cui = curie_s.split(":", 1)[1]
            id_info = entry.get("id", {}) if isinstance(entry.get("id"), dict) else {}
            label = str(id_info.get("label") or "").strip()
            raw_types = entry.get("type", []) if isinstance(entry.get("type"), list) else []
            types = [str(t).strip() for t in raw_types if str(t).strip()]
            primary_type = types[0] if types else ""
            md_by_cui[cui] = {
                "display_name": label or f"UMLS:{cui}",
                "primary_type": primary_type,
                "types": "|".join(types),
            }

    fallback = build_fallback_semmed_metadata(semmed_concept_csv_path, target_cuis=target_cuis)
    for cui in target_cuis:
        if cui not in md_by_cui:
            md_by_cui[cui] = fallback.get(
                cui,
                {
                    "display_name": f"UMLS:{cui}",
                    "primary_type": "",
                    "types": "",
                },
            )
    return md_by_cui


def is_biolink_disease_like(primary_type: str, types_pipe: str) -> bool:
    values = " ".join([primary_type or "", types_pipe or ""]).lower()
    disease_tokens = ("disease", "syndrome", "phenotypicfeature", "pathologicalprocess")
    return any(tok in values for tok in disease_tokens)


def is_text_disease_like(value: str) -> bool:
    v = (value or "").strip().lower()
    if not v:
        return False
    return any(tok in v for tok in ("disease", "disorder", "syndrome", "patholog"))


def intervention_seed_exclusion_reason(
    display_name: str,
    primary_type: str,
    types_pipe: str,
) -> str:
    types_list = [x.strip() for x in (types_pipe or "").split("|") if x and x.strip()]
    type_tokens = {
        t.strip().lower()
        for t in ([primary_type] + types_list)
        if t and str(t).strip()
    }
    n = (display_name or "").strip().lower()
    if "biolink:disease" in type_tokens or "semmed:mobd" in type_tokens:
        return "disease_type"
    if re.search(r"\b(disorder|abuse|dependence|syndrome)\b", n):
        return "disease_lexical"
    return ""


def build_intervention_seed_qc_rows(
    semmed_nodes: List[Dict[str, object]],
    semmed_md_by_cui: Dict[str, dict],
) -> Tuple[List[List[object]], Set[str]]:
    rows: List[List[object]] = [[
        "seed_umls_id",
        "seed_display_name",
        "seed_primary_type",
        "seed_types",
        "exclude_reason",
        "kept_as_intervention_seed",
    ]]
    excluded_umls: Set[str] = set()
    seen: Set[str] = set()
    for node in semmed_nodes:
        source_key = str(node.get("source_key") or "")
        if not source_key.startswith("UMLS:"):
            continue
        umls_id = source_key
        if umls_id in seen:
            continue
        seen.add(umls_id)
        cui = umls_id.split(":", 1)[1]
        md = semmed_md_by_cui.get(cui, {})
        display_name = str(md.get("display_name") or umls_id)
        primary = str(md.get("primary_type") or "")
        types = str(md.get("types") or "")
        reason = intervention_seed_exclusion_reason(
            display_name=display_name,
            primary_type=primary,
            types_pipe=types,
        )
        if reason:
            excluded_umls.add(umls_id)
        rows.append(
            [
                umls_id,
                display_name,
                primary,
                types,
                reason,
                "false" if reason else "true",
            ]
        )
    return rows, excluded_umls


def build_semmed_onehop_rows(
    semmed_adjacent_rows: List[List[str]],
    seed_cuis: Set[str],
    semmed_md_by_cui: Dict[str, dict],
    label_by_umls: Dict[str, str],
) -> List[List[object]]:
    out: List[List[object]] = [[
        "seed_umls_id",
        "cm_intervention_presence_label",
        "predicate",
        "frequency",
        "seed_side",
        "start_raw",
        "end_raw",
        "destination_umls_id",
        "destination_display_name",
        "destination_primary_type",
        "destination_types",
        "destination_is_disease_like",
    ]]
    for row in semmed_adjacent_rows[1:]:
        if len(row) < 3:
            continue
        start_raw = (row[0] or "").strip()
        end_raw = (row[1] or "").strip()
        predicate = (row[2] or "").strip()
        frequency = (row[3] or "").strip() if len(row) > 3 else ""
        start_cuis = semmed_cuis_from_edge_endpoint(start_raw)
        end_cuis = semmed_cuis_from_edge_endpoint(end_raw)
        start_seeds = sorted(start_cuis.intersection(seed_cuis))
        end_seeds = sorted(end_cuis.intersection(seed_cuis))

        if start_seeds and not end_seeds:
            dest_candidates = sorted(end_cuis)
            for seed_cui in start_seeds:
                for dest_cui in dest_candidates:
                    md = semmed_md_by_cui.get(dest_cui, {})
                    primary = str(md.get("primary_type") or "")
                    types = str(md.get("types") or "")
                    out.append(
                        [
                            f"UMLS:{seed_cui}",
                            label_by_umls.get(f"UMLS:{seed_cui}", "CM Intervention SemMed"),
                            predicate,
                            frequency,
                            "start",
                            start_raw,
                            end_raw,
                            f"UMLS:{dest_cui}",
                            str(md.get("display_name") or f"UMLS:{dest_cui}"),
                            primary,
                            types,
                            "true" if is_biolink_disease_like(primary, types) else "false",
                        ]
                    )
        elif end_seeds and not start_seeds:
            dest_candidates = sorted(start_cuis)
            for seed_cui in end_seeds:
                for dest_cui in dest_candidates:
                    md = semmed_md_by_cui.get(dest_cui, {})
                    primary = str(md.get("primary_type") or "")
                    types = str(md.get("types") or "")
                    out.append(
                        [
                            f"UMLS:{seed_cui}",
                            label_by_umls.get(f"UMLS:{seed_cui}", "CM Intervention SemMed"),
                            predicate,
                            frequency,
                            "end",
                            start_raw,
                            end_raw,
                            f"UMLS:{dest_cui}",
                            str(md.get("display_name") or f"UMLS:{dest_cui}"),
                            primary,
                            types,
                            "true" if is_biolink_disease_like(primary, types) else "false",
                        ]
                    )
    return out


def build_ikraph_onehop_rows(
    ikraph_adjacent_rows: List[List[str]],
    filtered_ikraph_nodes: List[Dict[str, object]],
    node_umls_hits: Dict[Tuple[str, str], List[str]],
    label_by_umls: Dict[str, str],
) -> List[List[object]]:
    out: List[List[object]] = [[
        "seed_ikraph_id",
        "seed_umls_id",
        "cm_intervention_presence_label",
        "predicate_label",
        "relationship_type",
        "direction",
        "score",
        "prob",
        "rel_id",
        "destination_ikraph_id",
        "destination_name",
        "destination_type",
        "destination_subtype",
        "destination_is_disease_like",
        "source",
    ]]
    seed_ikraph_ids = {
        str(n.get("source_key") or "")
        for n in filtered_ikraph_nodes
        if str(n.get("dataset") or "") == "ikraph" and str(n.get("source_key") or "")
    }
    if not ikraph_adjacent_rows:
        return out
    header = [str(x).strip() for x in ikraph_adjacent_rows[0]]
    col_idx = {name: i for i, name in enumerate(header)}

    def get_col(row: List[str], name: str, default: str = "") -> str:
        i = col_idx.get(name)
        if i is None or i >= len(row):
            return default
        return str(row[i]).strip()

    for row in ikraph_adjacent_rows[1:]:
        n1 = get_col(row, "node_one_id")
        n2 = get_col(row, "node_two_id")
        n1_seed = n1 in seed_ikraph_ids
        n2_seed = n2 in seed_ikraph_ids
        if not n1_seed and not n2_seed:
            continue

        def emit(seed_id: str, dest_id: str, dest_name: str, dest_type: str, dest_subtype: str):
            umls_hits = node_umls_hits.get(("ikraph", seed_id), [])
            for umls_id in umls_hits:
                out.append(
                    [
                        seed_id,
                        umls_id,
                        label_by_umls.get(umls_id, "CM Intervention iKraph"),
                        get_col(row, "predicate_label"),
                        get_col(row, "relationship_type"),
                        get_col(row, "direction"),
                        get_col(row, "score"),
                        get_col(row, "prob"),
                        get_col(row, "relID"),
                        dest_id,
                        dest_name,
                        dest_type,
                        dest_subtype,
                        "true" if is_text_disease_like(dest_type) else "false",
                        get_col(row, "source"),
                    ]
                )

        if n1_seed and not n2_seed:
            emit(
                seed_id=n1,
                dest_id=n2,
                dest_name=get_col(row, "node_two_name"),
                dest_type=get_col(row, "node_two_type"),
                dest_subtype=get_col(row, "node_two_subtype"),
            )
        elif n2_seed and not n1_seed:
            emit(
                seed_id=n2,
                dest_id=n1,
                dest_name=get_col(row, "node_one_name"),
                dest_type=get_col(row, "node_one_type"),
                dest_subtype=get_col(row, "node_one_subtype"),
            )
    return out


def build_umls_biolink_verification_rows(
    all_umls_ids: Set[str],
    intervention_seed_umls_ids: Set[str],
    onehop_destination_umls_ids: Set[str],
    label_by_umls: Dict[str, str],
    semmed_md_by_cui: Dict[str, dict],
) -> List[List[object]]:
    out: List[List[object]] = [[
        "umls_id",
        "cm_intervention_presence_label",
        "is_intervention_seed",
        "is_onehop_destination",
        "display_name",
        "primary_biolink_type",
        "biolink_types",
        "is_disease_like",
    ]]
    for umls_id in sorted(all_umls_ids):
        cui = umls_id.split(":", 1)[1] if umls_id.startswith("UMLS:") else umls_id
        md = semmed_md_by_cui.get(cui, {})
        primary = str(md.get("primary_type") or "")
        types = str(md.get("types") or "")
        out.append(
            [
                umls_id,
                label_by_umls.get(umls_id, ""),
                "true" if umls_id in intervention_seed_umls_ids else "false",
                "true" if umls_id in onehop_destination_umls_ids else "false",
                str(md.get("display_name") or umls_id),
                primary,
                types,
                "true" if is_biolink_disease_like(primary, types) else "false",
            ]
        )
    return out


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
        default="/Users/drshika2/neo4jexploration/semmed_data/connections.csv",
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
    parser.add_argument(
        "--include-embedding-mappings",
        action="store_true",
        help="If set, allow embedding-assisted term support and embedding-only match augmentation.",
    )
    parser.add_argument(
        "--focus-intervention-umls-only",
        action="store_true",
        help="Restrict to COMBO intervention subtree classes that have only UMLS IDs, then keep only SemMed/iKraph nodes carrying those UMLS IDs.",
    )
    parser.add_argument(
        "--intervention-root-fragment",
        default="Complementary_Medicine_Intervention",
        help="COMBO class fragment used as root for intervention subtree filtering.",
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
        allow_embedding_support=args.include_embedding_mappings,
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
        allow_embedding_support=args.include_embedding_mappings,
        denylist=denylist,
        semmed_labels={},
    )

    # Default behavior is exact-only mappings. Embedding augmentation is opt-in.
    if args.include_embedding_mappings:
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

    eligible_intervention_combo_uris: Set[str] = set()
    eligible_intervention_umls_ids: Set[str] = set()
    intervention_presence_rows: List[List[object]] = []
    intervention_label_by_umls: Dict[str, str] = {}
    intervention_node_umls_hits: Dict[Tuple[str, str], List[str]] = {}
    intervention_subtree_uris: Set[str] = set()
    intervention_seed_qc_rows: List[List[object]] = []
    intervention_seed_excluded_count = 0

    if args.focus_intervention_umls_only:
        intervention_subtree_raw = build_combo_intervention_subtree_from_owl(
            owl_path=combo_owl,
            root_fragment=args.intervention_root_fragment,
        )
        intervention_subtree_uris = {
            combo_uri_alias_to_canonical.get(uri, uri) for uri in intervention_subtree_raw
        }
        eligible_intervention_combo_uris, eligible_intervention_umls_ids = select_intervention_umls_only_combo_uris(
            intervention_combo_uris=intervention_subtree_uris,
            combo_uri_to_ids=combo_uri_to_ids,
        )
        semmed_matched, semmed_hits = filter_matched_nodes_for_combo_uris_and_umls(
            matched_nodes=semmed_matched,
            allowed_combo_uris=eligible_intervention_combo_uris,
            allowed_umls_ids=eligible_intervention_umls_ids,
        )
        semmed_seed_cuis = {
            str(node.get("source_key")).split(":", 1)[1]
            for node in semmed_matched
            if str(node.get("source_key") or "").startswith("UMLS:")
        }
        semmed_seed_md = load_semmed_metadata_for_cuis(
            semmed_normalized_path=semmed_normalized,
            semmed_concept_csv_path=semmed_concepts,
            target_cuis=semmed_seed_cuis,
        )
        intervention_seed_qc_rows, excluded_seed_umls = build_intervention_seed_qc_rows(
            semmed_nodes=semmed_matched,
            semmed_md_by_cui=semmed_seed_md,
        )
        intervention_seed_excluded_count = len(excluded_seed_umls)
        if excluded_seed_umls:
            semmed_matched = [
                node for node in semmed_matched
                if str(node.get("source_key") or "") not in excluded_seed_umls
            ]
            semmed_hits = {
                k: v for k, v in semmed_hits.items()
                if not (k[0] == "semmed" and k[1] in excluded_seed_umls)
            }
        ikraph_matched, ikraph_hits = filter_matched_nodes_for_combo_uris_and_umls(
            matched_nodes=ikraph_matched,
            allowed_combo_uris=eligible_intervention_combo_uris,
            allowed_umls_ids=eligible_intervention_umls_ids,
        )
        intervention_node_umls_hits = {**semmed_hits, **ikraph_hits}
        intervention_presence_rows, intervention_label_by_umls = build_cm_intervention_presence_rows(
            semmed_nodes=semmed_matched,
            ikraph_nodes=ikraph_matched,
            node_umls_hits=intervention_node_umls_hits,
        )

    all_matched = semmed_matched + ikraph_matched
    cross_links = build_cross_normalization_links(all_matched)

    matched_semmed_cuis: Set[str] = set()
    for node in semmed_matched:
        source_key = str(node.get("source_key") or "").strip()
        if source_key.startswith("UMLS:"):
            matched_semmed_cuis.add(source_key.split(":", 1)[1])

    matched_ikraph_identifiers: Set[str] = set()
    for node in ikraph_matched:
        source_key = str(node.get("source_key") or "").strip()
        if source_key:
            matched_ikraph_identifiers.add(source_key)

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

    semmed_onehop_rows: List[List[object]] = []
    ikraph_onehop_rows: List[List[object]] = []
    umls_biolink_verification_rows: List[List[object]] = []
    if args.focus_intervention_umls_only:
        intervention_seed_cuis = {
            x.split(":", 1)[1]
            for x in eligible_intervention_umls_ids
            if x.startswith("UMLS:")
        }
        semmed_cuis_in_adjacent: Set[str] = set()
        for row in semmed_adjacent[1:]:
            if len(row) < 2:
                continue
            semmed_cuis_in_adjacent.update(semmed_cuis_from_edge_endpoint(row[0]))
            semmed_cuis_in_adjacent.update(semmed_cuis_from_edge_endpoint(row[1]))
        semmed_md_by_cui = load_semmed_metadata_for_cuis(
            semmed_normalized_path=semmed_normalized,
            semmed_concept_csv_path=semmed_concepts,
            target_cuis=semmed_cuis_in_adjacent.union(intervention_seed_cuis),
        )
        semmed_onehop_rows = build_semmed_onehop_rows(
            semmed_adjacent_rows=semmed_adjacent,
            seed_cuis=intervention_seed_cuis,
            semmed_md_by_cui=semmed_md_by_cui,
            label_by_umls=intervention_label_by_umls,
        )
        ikraph_onehop_rows = build_ikraph_onehop_rows(
            ikraph_adjacent_rows=ikraph_adjacent,
            filtered_ikraph_nodes=ikraph_matched,
            node_umls_hits=intervention_node_umls_hits,
            label_by_umls=intervention_label_by_umls,
        )
        onehop_destination_umls_ids = {
            str(row[7])
            for row in semmed_onehop_rows[1:]
            if len(row) > 7 and str(row[7]).startswith("UMLS:")
        }
        umls_biolink_verification_rows = build_umls_biolink_verification_rows(
            all_umls_ids=eligible_intervention_umls_ids.union(onehop_destination_umls_ids),
            intervention_seed_umls_ids=eligible_intervention_umls_ids,
            onehop_destination_umls_ids=onehop_destination_umls_ids,
            label_by_umls=intervention_label_by_umls,
            semmed_md_by_cui=semmed_md_by_cui,
        )

        write_tsv(output_dir / "combo_cm_intervention_presence.tsv", intervention_presence_rows)
        write_tsv(output_dir / "combo_cm_intervention_semmed_onehop.tsv", semmed_onehop_rows)
        write_tsv(output_dir / "combo_cm_intervention_ikraph_onehop.tsv", ikraph_onehop_rows)
        write_tsv(output_dir / "combo_umls_biolink_type_verification.tsv", umls_biolink_verification_rows)
        write_tsv(output_dir / "combo_cm_intervention_seed_qc.tsv", intervention_seed_qc_rows)

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
        "include_embedding_mappings": bool(args.include_embedding_mappings),
        "focus_intervention_umls_only": bool(args.focus_intervention_umls_only),
        "intervention_root_fragment": args.intervention_root_fragment,
        "intervention_subtree_combo_uris": len(intervention_subtree_uris),
        "intervention_umls_only_combo_uris": len(eligible_intervention_combo_uris),
        "intervention_umls_ids": len(eligible_intervention_umls_ids),
        "cm_intervention_presence_rows": max(len(intervention_presence_rows) - 1, 0),
        "cm_intervention_semmed_onehop_rows": max(len(semmed_onehop_rows) - 1, 0),
        "cm_intervention_ikraph_onehop_rows": max(len(ikraph_onehop_rows) - 1, 0),
        "umls_biolink_verification_rows": max(len(umls_biolink_verification_rows) - 1, 0),
        "cm_intervention_seed_qc_rows": max(len(intervention_seed_qc_rows) - 1, 0),
        "intervention_seed_excluded_count": intervention_seed_excluded_count,
    }
    with (output_dir / "combo_subgraph_summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    print("Wrote COMBO subgraph outputs to", output_dir)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
