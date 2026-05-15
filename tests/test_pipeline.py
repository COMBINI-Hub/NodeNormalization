"""
Unit tests for the KG build pipeline steps.

Each test calls a step function directly with small fixture data in
tests/fixtures/pipeline/.  No external services, no Docker, no Redis needed.

Run with:  pytest tests/test_pipeline.py -v
"""
from __future__ import annotations

import csv
import json
import sys
import warnings
from pathlib import Path
from typing import Dict

import pytest

# Allow imports from scripts/
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from pipeline_steps.preprocess import preprocess_ikraph_edges, preprocess_semmed_edges
from pipeline_steps.reltype_map import generate_reltype_map, load_reltype_map
from pipeline_steps.build_import_csvs import (
    IKRAPH_SOURCE_TO_INFORES,
    IKRAPH_PUBMED_INFORES,
    SEMMED_INFORES,
    validate_biolink_predicate,
    build_ikraph_edges,
    build_semmed_edges,
    _materialize_semmed_nodes,
    _load_norm_json,
)
from pipeline_steps.restructure import restructure_for_neo4j
from pipeline_steps.normalize import normalize_nodes

FIXTURES = Path(__file__).parent / "fixtures" / "pipeline"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp(tmp_path: Path) -> Path:
    return tmp_path


# ---------------------------------------------------------------------------
# preprocess tests
# ---------------------------------------------------------------------------

class TestPreprocessIKraphEdges:
    def test_self_loops_removed(self, tmp: Path) -> None:
        out = tmp / "ikraph_cleaned.csv"
        preprocess_ikraph_edges(FIXTURES / "mini_DBRelations.json", out)
        rows = _read_csv(out)
        ids = [(r["node_one_id"], r["node_two_id"]) for r in rows]
        assert ("105", "105") not in ids, "Self-loop should be removed"

    def test_duplicates_removed(self, tmp: Path) -> None:
        out = tmp / "ikraph_cleaned.csv"
        preprocess_ikraph_edges(FIXTURES / "mini_DBRelations.json", out)
        rows = _read_csv(out)
        # (100, 200, 2) appears twice in the fixture; should appear once
        dupes = [r for r in rows if r["node_one_id"] == "100" and r["node_two_id"] == "200" and r["relationship_type"] == "2"]
        assert len(dupes) == 1

    def test_direction_field_preserved(self, tmp: Path) -> None:
        out = tmp / "ikraph_cleaned.csv"
        preprocess_ikraph_edges(FIXTURES / "mini_DBRelations.json", out)
        rows = _read_csv(out)
        by_n1_n2 = {(r["node_one_id"], r["node_two_id"]): r for r in rows}
        assert by_n1_n2[("101", "202")]["direction"] == "12"
        assert by_n1_n2[("103", "104")]["direction"] == "21"
        assert by_n1_n2[("100", "200")]["direction"] == "0"

    def test_source_field_preserved(self, tmp: Path) -> None:
        out = tmp / "ikraph_cleaned.csv"
        preprocess_ikraph_edges(FIXTURES / "mini_DBRelations.json", out)
        rows = _read_csv(out)
        sources = {r["source"] for r in rows}
        assert "Hetionet" in sources
        assert "PubChem" in sources
        assert "primeKG" in sources


class TestPreprocessSemmedEdges:
    def test_self_loops_removed(self, tmp: Path) -> None:
        out = tmp / "semmed_cleaned.csv"
        preprocess_semmed_edges(FIXTURES / "mini_connections.csv", out)
        rows = _read_csv(out)
        assert not any(r[":START_ID"] == r[":END_ID"] for r in rows)

    def test_duplicates_removed(self, tmp: Path) -> None:
        out = tmp / "semmed_cleaned.csv"
        preprocess_semmed_edges(FIXTURES / "mini_connections.csv", out)
        rows = _read_csv(out)
        # C0000001,C0000002,TREATS appears twice in the fixture
        dupes = [r for r in rows if r[":START_ID"] == "C0000001" and r[":END_ID"] == "C0000002" and r[":TYPE"] == "TREATS"]
        assert len(dupes) == 1


# ---------------------------------------------------------------------------
# reltype_map tests
# ---------------------------------------------------------------------------

class TestLoadReltypeMap:
    def test_basic_load(self) -> None:
        rmap = load_reltype_map(FIXTURES / "mini_reltype_map.csv")
        assert "2" in rmap
        pred, sin = rmap["2"]
        assert pred == "biolink:positively_regulates"
        assert sin is True

    def test_subject_is_node_one_false(self) -> None:
        rmap = load_reltype_map(FIXTURES / "mini_reltype_map.csv")
        pred, sin = rmap["7"]
        assert sin is False

    def test_missing_column_defaults_true(self, tmp: Path) -> None:
        # Write a CSV without subject_is_node_one column
        csv_path = tmp / "rmap_no_sin.csv"
        with csv_path.open("w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["int_rep", "ikraph_relation_type", "proposed_biolink_predicate", "source"])
            w.writerow(["1", "rel_one", "biolink:related_to", "default"])
        rmap = load_reltype_map(csv_path)
        _, sin = rmap["1"]
        assert sin is True


# ---------------------------------------------------------------------------
# validate_biolink_predicate tests
# ---------------------------------------------------------------------------

class TestValidateBiolinkPredicate:
    def test_known_predicate_passes(self) -> None:
        assert validate_biolink_predicate("biolink:treats") is True

    def test_bad_format_warns(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = validate_biolink_predicate("biolink:TREATS")
        assert result is False
        assert any("Non-canonical" in str(x.message) for x in w)

    def test_unknown_predicate_warns(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = validate_biolink_predicate("biolink:some_totally_new_predicate_xyz")
        assert result is False
        assert any("Unknown" in str(x.message) for x in w)

    def test_missing_biolink_prefix_warns(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = validate_biolink_predicate("treats")
        assert result is False


# ---------------------------------------------------------------------------
# build_ikraph_edges direction + PMID + provided_by tests
# ---------------------------------------------------------------------------

class TestBuildIkraphEdges:
    def _make_node_map_db(self, tmp: Path) -> Path:
        """Build a SQLite node map from mini_NER_ID_dict.json + mini_ikraph_normalized.json."""
        import sqlite3
        from pipeline_steps.build_import_csvs import _init_sqlite, _materialize_ikraph_nodes

        norm_map = _load_norm_json(FIXTURES / "mini_ikraph_normalized.json")
        node_map_db = tmp / "ikraph_node_map.sqlite"
        _materialize_ikraph_nodes(FIXTURES / "mini_NER_ID_dict.json", norm_map, node_map_db)
        return node_map_db

    def test_direction_undirected_keeps_order(self, tmp: Path) -> None:
        node_map_db = self._make_node_map_db(tmp)
        rmap = load_reltype_map(FIXTURES / "mini_reltype_map.csv")
        build_ikraph_edges(
            FIXTURES / "mini_DBRelations.json",
            FIXTURES / "mini_PubMedList.json",
            node_map_db,
            rmap,
            tmp,
        )
        edges = _read_csv(tmp / "ikraph_edges_normalized.csv")
        # direction=0: (100→200) mapped to (NCBIGene:100 → MONDO:200)
        undirected = [e for e in edges if e[":START_ID"] == "NCBIGene:100" and e[":END_ID"] == "MONDO:200"]
        assert undirected, "Undirected edge 100→200 should be written as-is"

    def test_direction_21_swaps_subject_object(self, tmp: Path) -> None:
        """
        Direction=21 on record (103→104) means node_two is the actual subject.
        After the swap the edge should appear as (NCBIGene:104 → NCBIGene:103).
        """
        node_map_db = self._make_node_map_db(tmp)
        rmap = load_reltype_map(FIXTURES / "mini_reltype_map.csv")
        build_ikraph_edges(
            FIXTURES / "mini_DBRelations.json",
            FIXTURES / "mini_PubMedList.json",
            node_map_db,
            rmap,
            tmp,
        )
        edges = _read_csv(tmp / "ikraph_edges_normalized.csv")
        # Original DB record: node_one=103, node_two=104, direction=21 → swap → 104→103
        swapped = [e for e in edges if e[":START_ID"] == "NCBIGene:104" and e[":END_ID"] == "NCBIGene:103"]
        wrong = [e for e in edges if e[":START_ID"] == "NCBIGene:103" and e[":END_ID"] == "NCBIGene:104"]
        assert swapped, "direction=21 should produce NCBIGene:104 → NCBIGene:103"
        assert not wrong, "direction=21 should NOT keep original order NCBIGene:103 → NCBIGene:104"

    def test_pmid_linked_to_direction_corrected_edge(self, tmp: Path) -> None:
        """
        PubMedList entry for (103, 104) has direction=21 and PMID 33333333.
        After swap the edge key is (NCBIGene:104, NCBIGene:103).
        The PMID must appear on that key, not the pre-swap key.
        """
        node_map_db = self._make_node_map_db(tmp)
        rmap = load_reltype_map(FIXTURES / "mini_reltype_map.csv")
        build_ikraph_edges(
            FIXTURES / "mini_DBRelations.json",
            FIXTURES / "mini_PubMedList.json",
            node_map_db,
            rmap,
            tmp,
        )
        edges = _read_csv(tmp / "ikraph_edges_normalized.csv")
        swapped_edge = next(
            (e for e in edges if e[":START_ID"] == "NCBIGene:104" and e[":END_ID"] == "NCBIGene:103"),
            None,
        )
        assert swapped_edge is not None
        assert "PMID:33333333" in swapped_edge["pmids:string[]"]

    def test_provided_by_hetionet(self, tmp: Path) -> None:
        node_map_db = self._make_node_map_db(tmp)
        rmap = load_reltype_map(FIXTURES / "mini_reltype_map.csv")
        build_ikraph_edges(
            FIXTURES / "mini_DBRelations.json",
            FIXTURES / "mini_PubMedList.json",
            node_map_db,
            rmap,
            tmp,
        )
        edges = _read_csv(tmp / "ikraph_edges_normalized.csv")
        hetionet_edges = [e for e in edges if "infores:hetionet" in e["provided_by:string[]"]]
        assert hetionet_edges, "Hetionet edges should have infores:hetionet in provided_by"

    def test_provided_by_pubchem(self, tmp: Path) -> None:
        node_map_db = self._make_node_map_db(tmp)
        rmap = load_reltype_map(FIXTURES / "mini_reltype_map.csv")
        build_ikraph_edges(
            FIXTURES / "mini_DBRelations.json",
            FIXTURES / "mini_PubMedList.json",
            node_map_db,
            rmap,
            tmp,
        )
        edges = _read_csv(tmp / "ikraph_edges_normalized.csv")
        pubchem_edges = [e for e in edges if "infores:pubchem" in e["provided_by:string[]"]]
        assert pubchem_edges, "PubChem edges should have infores:pubchem in provided_by"

    def test_pubmed_edges_use_ikraph_infores(self, tmp: Path) -> None:
        """
        PubMedList entry "106.107.2.0.12.MP" has no DB counterpart (nodes 106 and 107
        only appear in PubMedList.json).  After the node map is built for both nodes,
        that edge should be written with provided_by = infores:ikraph.
        """
        node_map_db = self._make_node_map_db(tmp)
        rmap = load_reltype_map(FIXTURES / "mini_reltype_map.csv")
        build_ikraph_edges(
            FIXTURES / "mini_DBRelations.json",
            FIXTURES / "mini_PubMedList.json",
            node_map_db,
            rmap,
            tmp,
        )
        edges = _read_csv(tmp / "ikraph_edges_normalized.csv")
        # Edge 106→107 is PubMed-only (direction=12, keep order) → infores:ikraph
        pubmed_only = [
            e for e in edges
            if e[":START_ID"] == "NCBIGene:106" and e[":END_ID"] == "NCBIGene:107"
        ]
        assert pubmed_only, "PubMed-only edge NCBIGene:106 → NCBIGene:107 should exist"
        assert pubmed_only[0]["provided_by:string[]"] == IKRAPH_PUBMED_INFORES

    def test_subject_is_node_one_false_triggers_extra_swap(self, tmp: Path) -> None:
        """
        reltype 7 in mini_reltype_map.csv has subject_is_node_one=False.
        Create a DB record with rel_type=7, direction=0 (undirected).
        The extra swap should make node_two the :START_ID.
        """
        import sqlite3
        from pipeline_steps.build_import_csvs import _init_sqlite

        # One DB record: n1=100, n2=200, rel=7, direction=0, source=Hetionet
        extra_db = [
            {
                "relID": "100.200.7.2.0.Hetionet",
                "node_one_id": "100",
                "node_two_id": "200",
                "direction": "0",
                "relationship_type": "7",
                "source": "Hetionet",
                "correlation_type": "2",
                "prob": 0.9, "method": "MA", "score": 1,
                "node_one_type": "Gene", "node_two_type": "Disease",
                "node_one_name": "GeneA", "node_two_name": "DiseaseB",
            }
        ]
        db_json = tmp / "extra_db.json"
        db_json.write_text(json.dumps(extra_db))
        empty_pubmed = tmp / "empty_pubmed.json"
        empty_pubmed.write_text("[]")

        node_map_db = self._make_node_map_db(tmp)
        rmap = load_reltype_map(FIXTURES / "mini_reltype_map.csv")
        build_ikraph_edges(db_json, empty_pubmed, node_map_db, rmap, tmp)

        edges = _read_csv(tmp / "ikraph_edges_normalized.csv")
        # subject_is_node_one=False + direction=0 → swap once → MONDO:200 → NCBIGene:100
        swapped = [e for e in edges if e[":START_ID"] == "MONDO:200" and e[":END_ID"] == "NCBIGene:100"]
        assert swapped, "subject_is_node_one=False should swap node_two → subject"


# ---------------------------------------------------------------------------
# build_semmed_edges provided_by test
# ---------------------------------------------------------------------------

class TestBuildSemmedEdges:
    def test_provided_by_semmeddb(self, tmp: Path) -> None:
        import gzip, io

        # Build a tiny gzip predication CSV
        lines = [
            "PREDICATION_ID,SENTENCE_ID,PMID,PREDICATE,SUBJECT_CUI,OBJECT_CUI\n",
            "1,1,12345678,TREATS,C0000002,C0000001\n",
            "2,2,99999999,INHIBITS,C0000003,C0000001\n",
        ]
        pred_gz = tmp / "predication.csv.gz"
        with gzip.open(pred_gz, "wt") as fh:
            fh.writelines(lines)

        norm_map = _load_norm_json(FIXTURES / "mini_semmed_normalized.json")
        from pipeline_steps.build_import_csvs import _materialize_semmed_nodes
        semmed_to_norm, _ = _materialize_semmed_nodes(norm_map)

        build_semmed_edges(pred_gz, semmed_to_norm, tmp)
        edges = _read_csv(tmp / "semmed_edges_normalized.csv")
        assert all(e["provided_by:string[]"] == SEMMED_INFORES for e in edges)

    def test_pmids_collected(self, tmp: Path) -> None:
        import gzip

        lines = [
            "PREDICATION_ID,SENTENCE_ID,PMID,PREDICATE,SUBJECT_CUI,OBJECT_CUI\n",
            "1,1,12345678,TREATS,C0000002,C0000001\n",
            "2,2,99999999,TREATS,C0000002,C0000001\n",
        ]
        pred_gz = tmp / "predication.csv.gz"
        with gzip.open(pred_gz, "wt") as fh:
            fh.writelines(lines)

        norm_map = _load_norm_json(FIXTURES / "mini_semmed_normalized.json")
        from pipeline_steps.build_import_csvs import _materialize_semmed_nodes
        semmed_to_norm, _ = _materialize_semmed_nodes(norm_map)
        build_semmed_edges(pred_gz, semmed_to_norm, tmp)
        edges = _read_csv(tmp / "semmed_edges_normalized.csv")
        treats_edges = [e for e in edges if e["semmed_predicate"] == "TREATS"]
        assert treats_edges
        pmids_field = treats_edges[0]["pmids:string[]"]
        assert "PMID:12345678" in pmids_field
        assert "PMID:99999999" in pmids_field


# ---------------------------------------------------------------------------
# _materialize_semmed_nodes tests
# ---------------------------------------------------------------------------

class TestMaterializeSemmedNodes:
    def test_maps_cui_to_norm_id(self) -> None:
        norm_map = _load_norm_json(FIXTURES / "mini_semmed_normalized.json")
        semmed_to_norm, nodes = _materialize_semmed_nodes(norm_map)
        assert semmed_to_norm["C0000001"] == "MONDO:0000001"
        assert semmed_to_norm["C0000002"] == "CHEBI:0000002"

    def test_node_metadata_populated(self) -> None:
        norm_map = _load_norm_json(FIXTURES / "mini_semmed_normalized.json")
        _, nodes = _materialize_semmed_nodes(norm_map)
        assert nodes["MONDO:0000001"]["name"] == "SomeDiseaseA"
        assert "biolink:Disease" in nodes["MONDO:0000001"]["types"]


# ---------------------------------------------------------------------------
# restructure_for_neo4j tests
# ---------------------------------------------------------------------------

class TestRestructureForNeo4j:
    def _make_minimal_csvs(self, src: Path) -> None:
        src.mkdir(parents=True, exist_ok=True)
        # Nodes
        for fname, label in [
            ("semmed_nodes_normalized.csv", "SemmedEntity"),
            ("ikraph_nodes_normalized.csv", "IKraphEntity"),
        ]:
            with (src / fname).open("w", newline="") as fh:
                w = csv.DictWriter(
                    fh,
                    fieldnames=["normalized_id:ID", "id", "name", "categories:string[]", "category:string[]", ":LABEL"],
                )
                w.writeheader()
                w.writerow({
                    "normalized_id:ID": "MONDO:1",
                    "id": "MONDO:1",
                    "name": "TestDisease",
                    "categories:string[]": "biolink:Disease;biolink:NamedThing",
                    "category:string[]": "biolink:Disease",
                    ":LABEL": "",
                })
        # Edges
        for fname in ["semmed_edges_normalized.csv", "ikraph_edges_normalized.csv"]:
            with (src / fname).open("w", newline="") as fh:
                w = csv.DictWriter(
                    fh,
                    fieldnames=[
                        ":START_ID", ":END_ID", ":TYPE",
                        "biolink_predicate", "semmed_predicate",
                        "frequency:int", "pmids:string[]",
                        "provided_by:string[]",
                        "subject", "object", "predicate",
                        "knowledge_level", "agent_type",
                    ],
                )
                w.writeheader()
                w.writerow({
                    ":START_ID": "MONDO:1",
                    ":END_ID": "CHEBI:1",
                    ":TYPE": "",
                    "biolink_predicate": "biolink:treats",
                    "semmed_predicate": "TREATS",
                    "frequency:int": "3",
                    "pmids:string[]": "PMID:123",
                    "provided_by:string[]": "infores:semmeddb",
                    "subject": "MONDO:1",
                    "object": "CHEBI:1",
                    "predicate": "biolink:treats",
                    "knowledge_level": "statistical_association",
                    "agent_type": "text_mining_agent",
                })

    def test_label_set_on_nodes(self, tmp: Path) -> None:
        src = tmp / "gen_med"
        dst = tmp / "gen_med_neo4j"
        self._make_minimal_csvs(src)
        restructure_for_neo4j(src, dst)
        rows = _read_csv(dst / "semmed_nodes_normalized.csv")
        assert rows[0][":LABEL"] == "SemmedEntity;Disease"

    def test_type_set_on_edges(self, tmp: Path) -> None:
        src = tmp / "gen_med"
        dst = tmp / "gen_med_neo4j"
        self._make_minimal_csvs(src)
        restructure_for_neo4j(src, dst)
        rows = _read_csv(dst / "semmed_edges_normalized.csv")
        assert rows[0][":TYPE"] == "TREATS"


# ---------------------------------------------------------------------------
# normalize_nodes skip test (no network required)
# ---------------------------------------------------------------------------

class TestNormalizeNodesSkip:
    def test_skips_when_outputs_exist(self, tmp: Path) -> None:
        """normalize_nodes should skip without calling NodeNorm if files already exist."""
        norm_dir = tmp / "norm"
        norm_dir.mkdir()
        semmed_json = norm_dir / "semmed_normalized_full.json"
        ikraph_json = norm_dir / "ikraph_normalized_full.json"
        semmed_json.write_text("{}")
        ikraph_json.write_text("{}")

        # Provide dummy paths — they won't be read since we skip
        dummy_gz = tmp / "pred.csv.gz"
        dummy_gz.write_bytes(b"")
        dummy_ner = tmp / "ner.json"
        dummy_ner.write_text("[]")

        out_semmed, out_ikraph = normalize_nodes(
            dummy_gz, dummy_ner, norm_dir,
            endpoint="http://should-not-be-called",
            force=False,
        )
        assert out_semmed == semmed_json
        assert out_ikraph == ikraph_json


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))
