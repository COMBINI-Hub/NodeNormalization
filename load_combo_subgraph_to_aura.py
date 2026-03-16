#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import os
import re
import socket
from pathlib import Path
from typing import Dict, Iterable, List
from urllib.parse import unquote, urlsplit

from neo4j import GraphDatabase

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


def chunks(items: List[dict], size: int) -> Iterable[List[dict]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def split_pipe(value: str) -> List[str]:
    if not value:
        return []
    return [v for v in value.split("|") if v]


def split_pipe_clean(value: str) -> List[str]:
    if not value:
        return []
    return [v for v in value.split("|") if v and v.strip() and v.strip().upper() != "N/A"]


def normalize_combo_key(value: str) -> str:
    return html.unescape(value.strip()).lower()


def extract_combo_fragment(uri: str) -> str:
    if "#" in uri:
        fragment = uri.rsplit("#", 1)[1]
    else:
        fragment = uri
    return html.unescape(unquote(fragment.strip()))


def is_combo_concept_id(value: str) -> bool:
    if not value:
        return False
    return all(part.isdigit() for part in value.split("."))


def is_cui_id(value: str) -> bool:
    if not value or not value.startswith("C"):
        return False
    return value[1:].isdigit()


def _extract_urls(text: str) -> List[str]:
    return re.findall(r"https?://[^\s<>\"]+", text or "")


def _extract_curies_from_text(text: str, prefix: str) -> List[str]:
    if not text:
        return []
    p = re.escape(prefix)
    return re.findall(rf"{p}:([A-Za-z0-9._-]+)", text)


def read_combo_cam_mappings(path: Path) -> Dict[str, dict]:
    by_id: Dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            combo_concept_id = (row.get("combo_concept_id") or "").strip()
            if not combo_concept_id or combo_concept_id in by_id:
                continue
            confidence_raw = (row.get("confidence") or "").strip()
            confidence = float(confidence_raw) if confidence_raw else None
            by_id[combo_concept_id] = {
                "cam_preferred_identifier": (row.get("preferred_identifier") or "").strip() or None,
                "cam_preferred_label": (row.get("preferred_label") or "").strip() or None,
                "cam_confidence": confidence,
                "cam_method": (row.get("method") or "").strip() or None,
                "cam_mapping_relation": (row.get("mapping_relation") or "").strip() or None,
                "cam_evidence": (row.get("evidence") or "").strip() or None,
            }
    return by_id


def read_combo_cam_mappings_by_label(path: Path) -> Dict[str, dict]:
    by_label: Dict[str, dict] = {}
    if not path.exists():
        return by_label
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            combo_label = html.unescape((row.get("combo_label") or "").strip())
            if not combo_label:
                continue
            key = normalize_combo_key(combo_label)
            if not key or key in by_label:
                continue
            confidence_raw = (row.get("confidence") or "").strip()
            confidence = float(confidence_raw) if confidence_raw else None
            by_label[key] = {
                "cam_preferred_identifier": (row.get("preferred_identifier") or "").strip() or None,
                "cam_preferred_label": (row.get("preferred_label") or "").strip() or None,
                "cam_confidence": confidence,
                "cam_method": (row.get("method") or "").strip() or None,
                "cam_mapping_relation": (row.get("mapping_relation") or "").strip() or None,
                "cam_evidence": (row.get("evidence") or "").strip() or None,
            }
    return by_label


def read_combo_metadata_from_owl(path: Path) -> Dict[str, dict]:
    """
    Build COMBO concept metadata directly from enriched OWL.
    """
    if not path.exists():
        return {}

    text = path.read_text(encoding="utf-8", errors="replace")
    uri_to_meta: Dict[str, dict] = {}

    def upsert_from_block(uri: str, block: str):
        uri = html.unescape(uri.strip())
        if not uri:
            return
        if uri not in uri_to_meta:
            frag = extract_combo_fragment(uri)
            uri_to_meta[uri] = {
                "combo_concept_id": None,
                "combo_label": None,
                "display_name": frag.replace("_", " ") if frag else None,
                "umls_ids": [],
                "mesh_ids": [],
                "synonyms": [],
                "provenance_urls": [],
                "hierarchy_depth": None,
            }
        meta = uri_to_meta[uri]

        label_match = re.search(r"<rdfs:label[^>]*>(.*?)</rdfs:label>", block, re.S)
        label = html.unescape(label_match.group(1).strip()) if label_match else None
        if label:
            meta["combo_label"] = meta["combo_label"] or label
            meta["display_name"] = label.replace("_", " ")

        for alt in re.findall(r"<skos:altLabel[^>]*>(.*?)</skos:altLabel>", block, re.S):
            alt_label = html.unescape(alt.strip())
            if alt_label and alt_label not in meta["synonyms"]:
                meta["synonyms"].append(alt_label)

        # provenance URLs from comments and notes
        for comment in re.findall(r"<rdfs:comment[^>]*>(.*?)</rdfs:comment>", block, re.S):
            for u in _extract_urls(html.unescape(comment)):
                if u not in meta["provenance_urls"]:
                    meta["provenance_urls"].append(u)
        for note in re.findall(r"<skos:note[^>]*>(.*?)</skos:note>", block, re.S):
            note_text = html.unescape(note)
            for u in _extract_urls(note_text):
                if u not in meta["provenance_urls"]:
                    meta["provenance_urls"].append(u)
            # handle note curies like MSH:Dxxxx
            for code in _extract_curies_from_text(note_text, "MSH"):
                curie = f"MESH:{code}"
                if curie not in meta["mesh_ids"]:
                    meta["mesh_ids"].append(curie)
            for code in _extract_curies_from_text(note_text, "MESH"):
                curie = f"MESH:{code}"
                if curie not in meta["mesh_ids"]:
                    meta["mesh_ids"].append(curie)
            for code in _extract_curies_from_text(note_text, "UMLS"):
                curie = f"UMLS:{code}"
                if curie not in meta["umls_ids"]:
                    meta["umls_ids"].append(curie)

        for cui in re.findall(r"<UMLS_CUI[^>]*>\s*(C\d{7})\s*</UMLS_CUI>", block):
            curie = f"UMLS:{cui}"
            if curie not in meta["umls_ids"]:
                meta["umls_ids"].append(curie)

        for umls in re.findall(r"identifiers\.org/umls/([A-Za-z0-9._-]+)", block, re.I):
            curie = f"UMLS:{umls}"
            if curie not in meta["umls_ids"]:
                meta["umls_ids"].append(curie)
        for mesh in re.findall(r"identifiers\.org/mesh/([A-Za-z0-9._-]+)", block, re.I):
            curie = f"MESH:{mesh}"
            if curie not in meta["mesh_ids"]:
                meta["mesh_ids"].append(curie)
        for mesh in re.findall(r"purl\.bioontology\.org/ontology/MESH/([A-Za-z0-9._-]+)", block, re.I):
            curie = f"MESH:{mesh}"
            if curie not in meta["mesh_ids"]:
                meta["mesh_ids"].append(curie)

    # owl:Class blocks
    for block in re.findall(r"<owl:Class\b[\s\S]*?</owl:Class>", text):
        m_uri = re.search(r'rdf:about="([^"]+)"', block)
        if not m_uri:
            continue
        upsert_from_block(m_uri.group(1), block)

    # rdf:Description blocks typed as owl:Class
    for block in re.findall(r"<rdf:Description\b[\s\S]*?</rdf:Description>", text):
        if 'rdf:resource="http://www.w3.org/2002/07/owl#Class"' not in block:
            continue
        m_uri = re.search(r'<rdf:Description\s+rdf:about="([^"]+)"', block)
        if not m_uri:
            continue
        upsert_from_block(m_uri.group(1), block)

    # Normalize empty lists to None for loader output consistency.
    for meta in uri_to_meta.values():
        for key in ("umls_ids", "mesh_ids", "synonyms", "provenance_urls"):
            if not meta[key]:
                meta[key] = None
    return uri_to_meta


def build_combo_concept_rows(
    seeds: List[dict],
    owl_metadata_by_uri: Dict[str, dict],
    cam_mappings_path: Path,
) -> tuple[List[dict], int]:
    all_uris = sorted({uri for row in seeds for uri in row["matched_combo_uris"] if uri})
    if not all_uris:
        return [], 0

    cam_by_id: Dict[str, dict] = {}
    if cam_mappings_path.exists():
        cam_by_id = read_combo_cam_mappings(cam_mappings_path)
    cam_by_label = read_combo_cam_mappings_by_label(cam_mappings_path)

    out: List[dict] = []
    matched_metadata = 0

    for uri in all_uris:
        fragment = extract_combo_fragment(uri)
        metadata = owl_metadata_by_uri.get(uri)

        row = {
            "uri": uri,
            "uri_prefix": uri.split("#", 1)[0] if "#" in uri else None,
            "combo_concept_id": None,
            "combo_label": None,
            "display_name": fragment.replace("_", " ") if fragment else None,
            "umls_ids": None,
            "mesh_ids": None,
            "synonyms": None,
            "provenance_urls": None,
            "hierarchy_depth": None,
            "cam_preferred_identifier": None,
            "cam_preferred_label": None,
            "cam_confidence": None,
            "cam_method": None,
            "cam_mapping_relation": None,
            "cam_evidence": None,
            "has_metadata": False,
        }

        if metadata is not None:
            row.update(metadata)
            row["has_metadata"] = True
            matched_metadata += 1

        concept_id = row.get("combo_concept_id")
        if concept_id and concept_id in cam_by_id:
            row.update(cam_by_id[concept_id])
        else:
            lbl = row.get("combo_label")
            if lbl:
                cam = cam_by_label.get(normalize_combo_key(lbl))
                if cam:
                    row.update(cam)

        out.append(row)

    return out, matched_metadata


def read_seed_nodes(path: Path) -> List[dict]:
    rows = []
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            rows.append(
                {
                    "seed_id": f"{row['dataset']}::{row['source_key']}",
                    "dataset": row["dataset"],
                    "source_key": row["source_key"],
                    "identifiers": split_pipe(row.get("identifiers", "")),
                    "terms": split_pipe(row.get("terms", "")),
                    "matched_by_identifiers": split_pipe(row.get("matched_by_identifiers", "")),
                    "matched_by_terms": split_pipe(row.get("matched_by_terms", "")),
                    "matched_combo_uris": split_pipe(row.get("matched_combo_uris", "")),
                }
            )
    return rows


def read_norm_links(path: Path) -> List[dict]:
    rows = []
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            rows.append(
                {
                    "left_seed_id": f"{row['left_dataset']}::{row['left_source_key']}",
                    "right_seed_id": f"{row['right_dataset']}::{row['right_source_key']}",
                    "term": row["normalization_term"],
                }
            )
    return rows


def read_embedding_links(path: Path) -> List[dict]:
    rows = []
    if not path.exists():
        return rows
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            confidence_raw = (row.get("embedding_confidence") or "").strip()
            try:
                confidence = float(confidence_raw)
            except ValueError:
                confidence = None
            rows.append(
                {
                    "seed_id": f"{row['dataset']}::{row['source_key']}",
                    "combo_uri": row["combo_uri"],
                    "embedding_term": row.get("embedding_term", ""),
                    "embedding_confidence": confidence,
                    "mapping_source": row.get("mapping_source", "embedding") or "embedding",
                }
            )
    return rows


def read_semmed_edges(path: Path) -> List[dict]:
    rows = []
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            rows.append(
                {
                    "start_id": row[":START_ID"],
                    "end_id": row[":END_ID"],
                    "predicate": row[":TYPE"],
                    "frequency": int(float(row.get("frequency%", "0") or 0)),
                }
            )
    return rows


def read_semmed_concepts(path: Path) -> List[dict]:
    rows = []
    if not path.exists():
        return rows
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            eq_count_raw = (row.get("equivalent_identifiers_count") or "0").strip()
            try:
                eq_count = int(eq_count_raw)
            except ValueError:
                eq_count = 0

            rows.append(
                {
                    "semmed_id": (row.get("semmed_id") or "").strip(),
                    "curie": (row.get("curie") or "").strip(),
                    "label": (row.get("label") or "").strip(),
                    "display_name": (row.get("display_name") or "").strip(),
                    "primary_type": (row.get("primary_type") or "").strip(),
                    "types": split_pipe(row.get("types", "")),
                    "equivalent_identifiers": split_pipe(row.get("equivalent_identifiers", "")),
                    "equivalent_labels": split_pipe(row.get("equivalent_labels", "")),
                    "equivalent_types": split_pipe(row.get("equivalent_types", "")),
                    "equivalent_identifiers_count": eq_count,
                    "normalized_found": ((row.get("normalized_found") or "").strip().lower() == "true"),
                }
            )
    return rows


def read_ikraph_edges(path: Path) -> List[dict]:
    rows = []
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            rows.append(
                {
                    "rel_id": row.get("relID", ""),
                    "node_one_id": row.get("node_one_id", ""),
                    "node_two_id": row.get("node_two_id", ""),
                    "node_one_type": row.get("node_one_type", ""),
                    "node_two_type": row.get("node_two_type", ""),
                    "node_one_name": row.get("node_one_name", ""),
                    "node_two_name": row.get("node_two_name", ""),
                    "node_one_subtype": row.get("node_one_subtype", ""),
                    "node_two_subtype": row.get("node_two_subtype", ""),
                    "direction": row.get("direction", ""),
                    "relationship_type": row.get("relationship_type", ""),
                    "predicate_label": row.get("predicate_label", ""),
                    "source": row.get("source", ""),
                    "method": row.get("method", ""),
                    "correlation_type": row.get("correlation_type", ""),
                    "prob": float(row.get("prob", "0") or 0),
                    "score": float(row.get("score", "0") or 0),
                }
            )
    return rows


def load_to_neo4j(
    driver,
    database: str,
    seeds: List[dict],
    embedding_links: List[dict],
    norm_links: List[dict],
    semmed_edges: List[dict],
    semmed_concepts: List[dict],
    ikraph_edges: List[dict],
    combo_concepts: List[dict],
    batch_size: int,
):
    with driver.session(database=database) as session:
        session.run(
            """
            CREATE CONSTRAINT combo_seed_id IF NOT EXISTS
            FOR (n:ComboSeed) REQUIRE n.seed_id IS UNIQUE
            """
        )
        session.run(
            """
            CREATE CONSTRAINT combo_concept_uri IF NOT EXISTS
            FOR (n:ComboConcept) REQUIRE n.uri IS UNIQUE
            """
        )
        session.run(
            """
            CREATE CONSTRAINT semmed_entity_id IF NOT EXISTS
            FOR (n:SemmedEntity) REQUIRE n.id IS UNIQUE
            """
        )
        session.run(
            """
            CREATE CONSTRAINT ikraph_entity_id IF NOT EXISTS
            FOR (n:IKraphEntity) REQUIRE n.id IS UNIQUE
            """
        )

        for batch in chunks(seeds, batch_size):
            session.run(
                """
                UNWIND $rows AS row
                MERGE (s:ComboSeed {seed_id: row.seed_id})
                SET s.dataset = row.dataset,
                    s.source_key = row.source_key,
                    s.identifiers = row.identifiers,
                    s.terms = row.terms,
                    s.display_name = coalesce(row.terms[0], row.source_key),
                    s.identifiers_count = size(row.identifiers),
                    s.terms_count = size(row.terms),
                    s.combo_matches_count = size(row.matched_combo_uris),
                    s.matched_by_identifiers = row.matched_by_identifiers,
                    s.matched_by_terms = row.matched_by_terms,
                    s.updated_at = datetime()
                WITH s, row
                UNWIND row.matched_combo_uris AS combo_uri
                MERGE (c:ComboConcept {uri: combo_uri})
                SET c.source = 'COMBO',
                    c.uri_prefix = coalesce(c.uri_prefix, split(combo_uri, '#')[0]),
                    c.display_name = coalesce(c.display_name, replace(last(split(combo_uri, '#')), '_', ' ')),
                    c.updated_at = datetime()
                MERGE (s)-[:MATCHES_COMBO]->(c)
                """,
                rows=batch,
            )

        # Enrich seed->concept links with embedding provenance/confidence when available.
        for batch in chunks(embedding_links, batch_size):
            session.run(
                """
                UNWIND $rows AS row
                MATCH (s:ComboSeed {seed_id: row.seed_id})
                MATCH (c:ComboConcept {uri: row.combo_uri})
                MERGE (s)-[r:MATCHES_COMBO]->(c)
                SET r.updated_at = datetime(),
                    r.embedding_match = true,
                    r.embedding_term = CASE
                        WHEN row.embedding_term IS NOT NULL AND row.embedding_term <> ''
                        THEN row.embedding_term
                        ELSE r.embedding_term
                    END,
                    r.embedding_confidence = CASE
                        WHEN row.embedding_confidence IS NULL THEN r.embedding_confidence
                        WHEN r.embedding_confidence IS NULL THEN row.embedding_confidence
                        WHEN row.embedding_confidence > r.embedding_confidence THEN row.embedding_confidence
                        ELSE r.embedding_confidence
                    END,
                    r.match_source = CASE
                        WHEN r.match_source IS NULL OR r.match_source = '' THEN row.mapping_source
                        WHEN r.match_source = row.mapping_source THEN r.match_source
                        ELSE 'mixed'
                    END
                """,
                rows=batch,
            )

        for batch in chunks(combo_concepts, batch_size):
            session.run(
                """
                UNWIND $rows AS row
                MERGE (c:ComboConcept {uri: row.uri})
                SET c.source = 'COMBO',
                    c.uri_prefix = coalesce(row.uri_prefix, c.uri_prefix),
                    c.combo_concept_id = coalesce(row.combo_concept_id, c.combo_concept_id),
                    c.combo_label = coalesce(row.combo_label, c.combo_label),
                    c.display_name = coalesce(row.display_name, c.display_name),
                    c.umls_ids = coalesce(row.umls_ids, c.umls_ids),
                    c.mesh_ids = coalesce(row.mesh_ids, c.mesh_ids),
                    c.synonyms = coalesce(row.synonyms, c.synonyms),
                    c.provenance_urls = coalesce(row.provenance_urls, c.provenance_urls),
                    c.hierarchy_depth = coalesce(row.hierarchy_depth, c.hierarchy_depth),
                    c.cam_preferred_identifier = coalesce(row.cam_preferred_identifier, c.cam_preferred_identifier),
                    c.cam_preferred_label = coalesce(row.cam_preferred_label, c.cam_preferred_label),
                    c.cam_confidence = coalesce(row.cam_confidence, c.cam_confidence),
                    c.cam_method = coalesce(row.cam_method, c.cam_method),
                    c.cam_mapping_relation = coalesce(row.cam_mapping_relation, c.cam_mapping_relation),
                    c.cam_evidence = coalesce(row.cam_evidence, c.cam_evidence),
                    c.has_metadata = row.has_metadata,
                    c.updated_at = datetime()
                """,
                rows=batch,
            )

        semmed_seed_links = []
        for row in seeds:
            if row["dataset"] != "semmed":
                continue
            source_key = row["source_key"]
            if source_key.startswith("UMLS:"):
                cui = source_key.split(":", 1)[1]
                preferred_name = row["terms"][0] if row["terms"] else ""
                semmed_seed_links.append(
                    {
                        "seed_id": row["seed_id"],
                        "semmed_id": cui,
                        "preferred_name": preferred_name,
                        "seed_source_key": source_key,
                    }
                )

        for batch in chunks(semmed_seed_links, batch_size):
            session.run(
                """
                UNWIND $rows AS row
                MATCH (s:ComboSeed {seed_id: row.seed_id})
                MERGE (e:SemmedEntity {id: row.semmed_id})
                SET e.source = 'SemMedDB',
                    e.id_namespace = 'UMLS',
                    e.id_curie = 'UMLS:' + row.semmed_id,
                    e.entity_type = 'UMLSConcept',
                    e.preferred_name = coalesce(e.preferred_name, row.preferred_name),
                    e.display_name = coalesce(e.display_name, row.preferred_name, 'UMLS:' + row.semmed_id),
                    e.seed_source_key = coalesce(e.seed_source_key, row.seed_source_key),
                    e.is_seed_mapped = true,
                    e.updated_at = datetime()
                MERGE (s)-[:MAPS_TO_SEMMED]->(e)
                """,
                rows=batch,
            )

        ikraph_seed_links = []
        for row in seeds:
            if row["dataset"] != "ikraph":
                continue
            source_key = row["source_key"]
            if not source_key:
                continue
            preferred_name = row["terms"][0] if row["terms"] else ""
            ikraph_seed_links.append(
                {
                    "seed_id": row["seed_id"],
                    "ikraph_id": source_key,
                    "preferred_name": preferred_name,
                    "seed_source_key": source_key,
                }
            )

        for batch in chunks(ikraph_seed_links, batch_size):
            session.run(
                """
                UNWIND $rows AS row
                MATCH (s:ComboSeed {seed_id: row.seed_id})
                MERGE (e:IKraphEntity {id: row.ikraph_id})
                SET e.source = coalesce(e.source, 'iKraph'),
                    e.name = coalesce(e.name, row.preferred_name),
                    e.display_name = coalesce(e.display_name, row.preferred_name, row.ikraph_id),
                    e.seed_source_key = coalesce(e.seed_source_key, row.seed_source_key),
                    e.is_seed_mapped = true,
                    e.updated_at = datetime()
                MERGE (s)-[:MAPS_TO_IKRAPH]->(e)
                """,
                rows=batch,
            )

        for batch in chunks(norm_links, batch_size):
            session.run(
                """
                UNWIND $rows AS row
                MATCH (a:ComboSeed {seed_id: row.left_seed_id})
                MATCH (b:ComboSeed {seed_id: row.right_seed_id})
                MERGE (a)-[r:NORMALIZED_WITH {term: row.term}]->(b)
                SET r.source = 'COMBO_NORMALIZATION',
                    r.updated_at = datetime()
                """,
                rows=batch,
            )

        for batch in chunks(semmed_edges, batch_size):
            session.run(
                """
                UNWIND $rows AS row
                MERGE (a:SemmedEntity {id: row.start_id})
                SET a.source = 'SemMedDB',
                    a.id_namespace = CASE
                        WHEN row.start_id =~ '^C[0-9]+$' THEN 'UMLS'
                        WHEN row.start_id =~ '^[0-9]+$' THEN 'SEMMED_RECORD'
                        ELSE 'UNKNOWN'
                    END,
                    a.id_curie = CASE
                        WHEN row.start_id =~ '^C[0-9]+$' THEN 'UMLS:' + row.start_id
                        ELSE row.start_id
                    END,
                    a.entity_type = CASE
                        WHEN row.start_id =~ '^C[0-9]+$' THEN 'UMLSConcept'
                        WHEN row.start_id =~ '^[0-9]+$' THEN 'SemMedRecord'
                        ELSE 'SemMedEntity'
                    END,
                    a.display_name = coalesce(
                        a.display_name,
                        a.preferred_name,
                        CASE
                            WHEN row.start_id =~ '^[0-9]+$' THEN 'SemMed record ' + row.start_id
                            WHEN row.start_id =~ '^C[0-9]+$' THEN 'UMLS:' + row.start_id
                            ELSE row.start_id
                        END
                    ),
                    a.updated_at = datetime()
                MERGE (b:SemmedEntity {id: row.end_id})
                SET b.source = 'SemMedDB',
                    b.id_namespace = CASE
                        WHEN row.end_id =~ '^C[0-9]+$' THEN 'UMLS'
                        WHEN row.end_id =~ '^[0-9]+$' THEN 'SEMMED_RECORD'
                        ELSE 'UNKNOWN'
                    END,
                    b.id_curie = CASE
                        WHEN row.end_id =~ '^C[0-9]+$' THEN 'UMLS:' + row.end_id
                        ELSE row.end_id
                    END,
                    b.entity_type = CASE
                        WHEN row.end_id =~ '^C[0-9]+$' THEN 'UMLSConcept'
                        WHEN row.end_id =~ '^[0-9]+$' THEN 'SemMedRecord'
                        ELSE 'SemMedEntity'
                    END,
                    b.display_name = coalesce(
                        b.display_name,
                        b.preferred_name,
                        CASE
                            WHEN row.end_id =~ '^[0-9]+$' THEN 'SemMed record ' + row.end_id
                            WHEN row.end_id =~ '^C[0-9]+$' THEN 'UMLS:' + row.end_id
                            ELSE row.end_id
                        END
                    ),
                    b.updated_at = datetime()
                MERGE (a)-[r:SEMMED_EDGE {predicate: row.predicate, start_id: row.start_id, end_id: row.end_id}]->(b)
                SET r.frequency = row.frequency,
                    r.source = 'SemMedDB',
                    r.updated_at = datetime()
                """,
                rows=batch,
            )

        for batch in chunks(semmed_concepts, batch_size):
            session.run(
                """
                UNWIND $rows AS row
                MERGE (e:SemmedEntity {id: row.semmed_id})
                SET e.source = 'SemMedDB',
                    e.id_namespace = 'UMLS',
                    e.id_curie = coalesce(row.curie, 'UMLS:' + row.semmed_id),
                    e.entity_type = 'UMLSConcept',
                    e.preferred_name = coalesce(row.label, e.preferred_name),
                    e.display_name = coalesce(row.display_name, row.label, e.display_name, 'UMLS:' + row.semmed_id),
                    e.primary_type = coalesce(row.primary_type, e.primary_type),
                    e.types = coalesce(row.types, e.types),
                    e.equivalent_identifiers = coalesce(row.equivalent_identifiers, e.equivalent_identifiers),
                    e.equivalent_labels = coalesce(row.equivalent_labels, e.equivalent_labels),
                    e.equivalent_types = coalesce(row.equivalent_types, e.equivalent_types),
                    e.equivalent_identifiers_count = coalesce(row.equivalent_identifiers_count, e.equivalent_identifiers_count),
                    e.normalized_found = row.normalized_found,
                    e.updated_at = datetime()
                """,
                rows=batch,
            )

        for batch in chunks(ikraph_edges, batch_size):
            session.run(
                """
                UNWIND $rows AS row
                MERGE (a:IKraphEntity {id: row.node_one_id})
                SET a.type = coalesce(a.type, row.node_one_type),
                    a.subtype = coalesce(a.subtype, row.node_one_subtype),
                    a.name = coalesce(a.name, row.node_one_name),
                    a.source = coalesce(a.source, 'iKraph'),
                    a.updated_at = datetime()
                MERGE (b:IKraphEntity {id: row.node_two_id})
                SET b.type = coalesce(b.type, row.node_two_type),
                    b.subtype = coalesce(b.subtype, row.node_two_subtype),
                    b.name = coalesce(b.name, row.node_two_name),
                    b.source = coalesce(b.source, 'iKraph'),
                    b.updated_at = datetime()
                MERGE (a)-[r:IKRAPH_EDGE {rel_id: row.rel_id}]->(b)
                SET r.relationship_type = row.relationship_type,
                    r.predicate_label = row.predicate_label,
                    r.direction = row.direction,
                    r.source = row.source,
                    r.method = row.method,
                    r.correlation_type = row.correlation_type,
                    r.prob = row.prob,
                    r.score = row.score,
                    r.updated_at = datetime()
                """,
                rows=batch,
            )


def clear_graph(driver, database: str, delete_batch: int = 20000) -> int:
    total_deleted = 0
    with driver.session(database=database) as session:
        while True:
            result = session.run(
                """
                MATCH (n)
                WITH n LIMIT $limit
                DETACH DELETE n
                RETURN count(n) AS deleted
                """,
                limit=delete_batch,
            ).single()
            deleted = int(result["deleted"])
            total_deleted += deleted
            if deleted == 0:
                break
    return total_deleted


def get_counts(driver, database: str) -> Dict[str, int]:
    queries = {
        "combo_seed_nodes": "MATCH (n:ComboSeed) RETURN count(n) AS c",
        "combo_concepts": "MATCH (n:ComboConcept) RETURN count(n) AS c",
        "normalization_links": "MATCH ()-[r:NORMALIZED_WITH]->() RETURN count(r) AS c",
        "embedding_match_links": "MATCH ()-[r:MATCHES_COMBO]->() WHERE r.embedding_match = true RETURN count(r) AS c",
        "semmed_entities": "MATCH (n:SemmedEntity) RETURN count(n) AS c",
        "ikraph_seed_links": "MATCH (:ComboSeed)-[r:MAPS_TO_IKRAPH]->(:IKraphEntity) RETURN count(r) AS c",
        "semmed_edges": "MATCH ()-[r:SEMMED_EDGE]->() RETURN count(r) AS c",
        "ikraph_entities": "MATCH (n:IKraphEntity) RETURN count(n) AS c",
        "ikraph_edges": "MATCH ()-[r:IKRAPH_EDGE]->() RETURN count(r) AS c",
    }
    out: Dict[str, int] = {}
    with driver.session(database=database) as session:
        for key, query in queries.items():
            out[key] = session.run(query).single()["c"]
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load COMBO subgraph TSV outputs into Neo4j Aura.")
    parser.add_argument("--env-file", default="/Users/drshika2/NodeNormalization/.env")
    parser.add_argument("--input-dir", default="/Users/drshika2/NodeNormalization/semmed_ikraph_normalized/combo_subgraph")
    parser.add_argument("--embedding-links-file", default="/Users/drshika2/NodeNormalization/semmed_ikraph_normalized/combo_subgraph/combo_embedding_links.tsv")
    parser.add_argument("--semmed-concepts-file", default="/Users/drshika2/NodeNormalization/semmed_ikraph_normalized/combo_subgraph/semmed_concepts_seed_adjacent.tsv")
    parser.add_argument("--combo-owl-file", default="/Users/drshika2/NodeNormalization/COMBO_20260115_with_skos.owl")
    parser.add_argument("--combo-cam-mappings-file", default="/Users/drshika2/NodeNormalization/combo_cam_lexicon_mappings.tsv")
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--max-seeds", type=int, default=0, help="Load at most this many ComboSeed rows (0 = all).")
    parser.add_argument("--max-semmed-edges", type=int, default=0, help="Load at most this many SemMed edges (0 = all).")
    parser.add_argument("--max-ikraph-edges", type=int, default=0, help="Load at most this many iKraph edges (0 = all).")
    parser.add_argument(
        "--semmed-concepts-only",
        action="store_true",
        help="Exclude non-concept SemMed nodes by keeping only CUI (C...) SemMed IDs and edges between CUIs.",
    )
    parser.add_argument("--clear-existing", action="store_true", help="Delete all existing nodes/relationships before loading.")
    parser.add_argument("--delete-batch-size", type=int, default=20000, help="Batch size for graph clearing when --clear-existing is used.")
    return parser.parse_args()


def validate_neo4j_uri(uri: str) -> None:
    cleaned_uri = uri.strip()
    parsed = urlsplit(cleaned_uri)

    if not parsed.scheme or not parsed.hostname:
        raise SystemExit(
            "Invalid NEO4J_URI. Expected format like 'neo4j+s://<instance>.databases.neo4j.io'. "
            f"Got: {cleaned_uri!r}"
        )

    if parsed.scheme not in {"neo4j", "neo4j+s", "neo4j+ssc", "bolt", "bolt+s", "bolt+ssc"}:
        raise SystemExit(
            "Unsupported NEO4J_URI scheme. Use one of: "
            "neo4j+s, neo4j+ssc, neo4j, bolt+s, bolt+ssc, bolt. "
            f"Got: {parsed.scheme!r}"
        )

    host = parsed.hostname
    port = parsed.port or 7687
    try:
        socket.getaddrinfo(host, port)
    except socket.gaierror as exc:
        raise SystemExit(
            "Cannot resolve the Neo4j host in NEO4J_URI. "
            f"Host: {host!r}, URI: {cleaned_uri!r}. "
            "This usually means the Aura connection URI is outdated or mistyped. "
            "Copy the current URI from Aura Console → Connect → Python and update NEO4J_URI."
        ) from exc


def main():
    args = parse_args()

    env_path = Path(args.env_file)
    if load_dotenv is not None and env_path.exists():
        load_dotenv(dotenv_path=env_path)

    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE") or "neo4j"

    if not uri or not username or not password:
        raise SystemExit("Missing one or more required env vars: NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD")

    uri = uri.strip()
    username = username.strip()
    password = password.strip()
    database = database.strip()

    validate_neo4j_uri(uri)

    input_dir = Path(args.input_dir)
    seed_path = input_dir / "combo_seed_nodes.tsv"
    embedding_links_path = Path(args.embedding_links_file)
    norm_path = input_dir / "combo_normalization_links.tsv"
    semmed_edges_path = input_dir / "semmed_edges_seed_adjacent.tsv"
    ikraph_edges_path = input_dir / "ikraph_edges_seed_adjacent.tsv"

    for p in (seed_path, norm_path, semmed_edges_path, ikraph_edges_path):
        if not p.exists():
            raise SystemExit(f"Missing required file: {p}")

    seeds = read_seed_nodes(seed_path)
    embedding_links = read_embedding_links(embedding_links_path)
    norm_links = read_norm_links(norm_path)
    semmed_edges = read_semmed_edges(semmed_edges_path)
    semmed_concepts = read_semmed_concepts(Path(args.semmed_concepts_file))
    ikraph_edges = read_ikraph_edges(ikraph_edges_path)

    if args.semmed_concepts_only:
        semmed_edges = [
            row for row in semmed_edges if is_cui_id(row["start_id"]) and is_cui_id(row["end_id"])
        ]
        semmed_concepts = [row for row in semmed_concepts if is_cui_id(row["semmed_id"])]

    combo_owl_path = Path(args.combo_owl_file)
    combo_cam_path = Path(args.combo_cam_mappings_file)
    combo_metadata_by_uri = read_combo_metadata_from_owl(combo_owl_path)
    combo_concepts, combo_metadata_matches = build_combo_concept_rows(
        seeds=seeds,
        owl_metadata_by_uri=combo_metadata_by_uri,
        cam_mappings_path=combo_cam_path,
    )

    if args.max_seeds > 0:
        seeds = seeds[: args.max_seeds]
        keep_seed_ids = {row["seed_id"] for row in seeds}
        norm_links = [
            row
            for row in norm_links
            if row["left_seed_id"] in keep_seed_ids and row["right_seed_id"] in keep_seed_ids
        ]

    if args.max_semmed_edges > 0:
        semmed_edges = sorted(
            semmed_edges,
            key=lambda row: (row["frequency"], row["start_id"], row["end_id"], row["predicate"]),
            reverse=True,
        )[: args.max_semmed_edges]

    if args.max_ikraph_edges > 0:
        ikraph_edges = sorted(
            ikraph_edges,
            key=lambda row: (row["score"], row["prob"], row["node_one_id"], row["node_two_id"], row["rel_id"]),
            reverse=True,
        )[: args.max_ikraph_edges]

    print(
        f"Rows: seeds={len(seeds)} embedding_links={len(embedding_links)} "
        f"norm_links={len(norm_links)} semmed_edges={len(semmed_edges)} ikraph_edges={len(ikraph_edges)}"
    )
    print(f"SemMed concepts metadata rows: {len(semmed_concepts)}")
    print(f"Combo concepts: unique_uris={len(combo_concepts)} metadata_matched={combo_metadata_matches}")

    driver = GraphDatabase.driver(uri, auth=(username, password))
    try:
        try:
            driver.verify_connectivity()
        except Exception as exc:
            raise SystemExit(
                "Neo4j connectivity check failed. Verify NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, "
                "and NEO4J_DATABASE in your .env file."
            ) from exc

        if args.clear_existing:
            deleted = clear_graph(driver=driver, database=database, delete_batch=args.delete_batch_size)
            print(f"Cleared existing graph. Deleted nodes: {deleted}")

        load_to_neo4j(
            driver=driver,
            database=database,
            seeds=seeds,
            embedding_links=embedding_links,
            norm_links=norm_links,
            semmed_edges=semmed_edges,
            semmed_concepts=semmed_concepts,
            ikraph_edges=ikraph_edges,
            combo_concepts=combo_concepts,
            batch_size=args.batch_size,
        )
        counts = get_counts(driver, database)
    finally:
        driver.close()

    print("Loaded counts:")
    for k, v in counts.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
