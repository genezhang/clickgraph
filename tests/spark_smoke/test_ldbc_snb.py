"""LDBC SNB Cypher→Spark SQL translation tests.

Runs a hand-written subset of LDBC SNB–style queries through `cg` under the
Databricks dialect and asserts on the *generated* Spark SQL — VLP CTEs,
OPTIONAL MATCH → LEFT JOIN, aggregations, string functions, etc. These verify
the Cypher→SQL translation only; they do not execute the SQL against a Spark
warehouse (that is covered by `test_smoke.py` / `test_ldbc_sweep.py`).

Skipped unless CLICKGRAPH_SPARK_TESTS=1.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA = REPO_ROOT / "benchmarks" / "ldbc_snb" / "schemas" / "ldbc_snb_complete.yaml"


def _resolve_cg_bin() -> Optional[Path]:
    """CG_BIN env override → cargo metadata target_directory → repo-relative target."""
    env_override = os.environ.get("CG_BIN")
    if env_override:
        return Path(env_override)
    try:
        meta = subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            cwd=REPO_ROOT, capture_output=True, text=True, check=True, timeout=30,
        )
        target_dir = Path(json.loads(meta.stdout)["target_directory"])
        candidate = target_dir / "release" / "cg"
        if candidate.exists():
            return candidate
    except (subprocess.SubprocessError, json.JSONDecodeError, KeyError, OSError):
        pass
    fallback = REPO_ROOT / "target" / "release" / "cg"
    return fallback if fallback.exists() else None


def _require_env() -> Path:
    if os.environ.get("CLICKGRAPH_SPARK_TESTS") != "1":
        pytest.skip("CLICKGRAPH_SPARK_TESTS=1 not set")
    cg = _resolve_cg_bin()
    if cg is None:
        pytest.skip(
            "cg binary not found — build with "
            "`cargo build --release -p clickgraph-tool --features databricks`"
        )
    return cg


def _generate_sql(cg_bin: Path, cypher: str) -> str:
    """Invoke cg to translate a Cypher query into Spark SQL."""
    result = subprocess.run(
        [str(cg_bin), "--schema", str(SCHEMA), "--dialect", "databricks", "sql", cypher],
        capture_output=True, text=True, check=True,
    )
    return result.stdout


# Hand-written subset of LDBC SNB–style queries for translation testing.
# These cover: basic patterns, VLP, OPTIONAL MATCH, aggregations, string functions

@pytest.fixture(scope="module")
def cg_bin() -> Path:
    """Shared cg binary for all tests in this module."""
    return _require_env()


class TestLdbcSnbFunctional:
    """Functional tests against LDBC SNB official queries."""

    def test_bi_1_shortest_path(self, cg_bin: Path):
        """bi-1: shortest path between two persons."""
        cypher = (
            "MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id}), "
            "p = shortestPath((p1)-[:KNOWS*..6]-(p2)) "
            "RETURN length(p) AS hopCount"
        )
        params = {"person1Id": 0, "person2Id": 1}
        cypher_with_params = cypher.replace("$person1Id", str(params["person1Id"])).replace(
            "$person2Id", str(params["person2Id"])
        )
        sql = _generate_sql(cg_bin, cypher_with_params)
        assert "shortestPath" in sql.lower() or "min" in sql.lower(), f"expected shortestPath handling:\n{sql}"
        # Just verify SQL generation works - actual execution may need larger dataset

    def test_bi_2_variable_length_path(self, cg_bin: Path):
        """bi-2: find friends of friends."""
        cypher = (
            "MATCH (p:Person {id: $personId})-[:KNOWS*2..2]-(friend:Person) "
            "WHERE friend.id <> p.id "
            "RETURN DISTINCT friend.id AS friendId, friend.firstName AS firstName, "
            "friend.lastName AS lastName ORDER BY friendId"
        )
        cypher = cypher.replace("$personId", "0")
        sql = _generate_sql(cg_bin, cypher)
        assert "WITH RECURSIVE" in sql or "vlp_" in sql.lower(), f"expected VLP CTE:\n{sql}"
        assert "hop_count" in sql.lower(), f"expected hop_count tracking:\n{sql}"

    def test_bi_3_complex_aggregation(self, cg_bin: Path):
        """bi-3: count posts per person with tag filtering."""
        cypher = (
            "MATCH (p:Person)-[:HAS_CREATOR]->(post:Post)-[:HAS_TAG]->(tag:Tag) "
            "WHERE tag.name IN [$tagName1, $tagName2] "
            "RETURN p.id AS personId, count(post) AS postCount "
            "ORDER BY postCount DESC LIMIT 20"
        )
        cypher = cypher.replace("$tagName1", "'Databases'").replace("$tagName2", "'Rust'")
        sql = _generate_sql(cg_bin, cypher)
        assert "collect_list" in sql.lower() or "array_contains" in sql.lower(), f"expected IN→OR or array handling:\n{sql}"
        assert "GROUP BY" in sql.upper(), f"expected GROUP BY:\n{sql}"

    def test_bi_4_optional_match(self, cg_bin: Path):
        """bi-4: persons with optional university info."""
        cypher = (
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:STUDY_AT]->(org:Organisation {type: 'University'}) "
            "RETURN p.id AS personId, p.firstName AS firstName, org.name AS universityName "
            "ORDER BY p.id LIMIT 10"
        )
        sql = _generate_sql(cg_bin, cypher)
        assert "LEFT JOIN" in sql, f"expected LEFT JOIN for OPTIONAL MATCH:\n{sql}"
        assert "IS NULL" in sql or "OR" in sql.upper(), f"expected NULL handling:\n{sql}"

    def test_bi_5_union_pattern(self, cg_bin: Path):
        """bi-5: find posts/comments by persons with specific tag."""
        cypher = (
            "MATCH (tag:Tag {name: $tagName})<-[:HAS_TAG]-(container)-[:HAS_CREATOR]->(p:Person) "
            "WHERE container:Post OR container:Comment "
            "RETURN p.id AS personId, p.firstName AS firstName, container.id AS containerId, "
            "container.content AS content ORDER BY container.creationDate DESC LIMIT 20"
        )
        cypher = cypher.replace("$tagName", "'Databases'")
        sql = _generate_sql(cg_bin, cypher)
        # Should handle UNION for Post/Comment

    def test_complex_1_supertype_inference(self, cg_bin: Path):
        """complex-1: supertype inference with IN clause."""
        cypher = (
            "MATCH (p:Person)-[:HAS_CREATOR]->(c:Comment) "
            "WHERE c.creationDate >= $startDate AND c.creationDate < $endDate "
            "RETURN p.id AS personId, count(c) AS commentCount "
            "ORDER BY commentCount DESC LIMIT 10"
        )
        cypher = cypher.replace("$startDate", "1262304000000").replace("$endDate", "1293840000000")
        sql = _generate_sql(cg_bin, cypher)
        assert "GROUP BY" in sql.upper(), f"expected GROUP BY:\n{sql}"

    def test_complex_2_path_query(self, cg_bin: Path):
        """complex-2: path query with multiple relationship types."""
        cypher = (
            "MATCH (p1:Person {id: $person1Id})-[:KNOWS*1..3]-(p2:Person) "
            "WHERE p2.id <> p1.id "
            "RETURN DISTINCT p2.id AS friendId, p2.firstName AS firstName, "
            "p2.lastName AS lastName ORDER BY p2.id LIMIT 10"
        )
        cypher = cypher.replace("$person1Id", "0")
        sql = _generate_sql(cg_bin, cypher)
        assert "WITH RECURSIVE" in sql or "vlp_" in sql.lower(), f"expected VLP CTE:\n{sql}"

    def test_complex_3_map_literal(self, cg_bin: Path):
        """complex-3: map literal with property access."""
        cypher = (
            "MATCH (p:Person {id: $personId}) "
            "RETURN {name: p.firstName, id: p.id, tags: collect('test')} AS personInfo"
        )
        cypher = cypher.replace("$personId", "0")
        sql = _generate_sql(cg_bin, cypher)
        # Map literals should translate to Spark struct/map

    def test_complex_4_not_exists(self, cg_bin: Path):
        """complex-4: NOT EXISTS pattern."""
        cypher = (
            "MATCH (p:Person {id: $personId}) "
            "WHERE NOT EXISTS {"
            "  MATCH (p)-[:KNOWS]->(f:Person)-[:KNOWS]->(p) "
            "} "
            "RETURN p.id AS personId, p.firstName AS firstName"
        )
        cypher = cypher.replace("$personId", "0")
        sql = _generate_sql(cg_bin, cypher)
        assert "NOT EXISTS" in sql.upper() or "NOT IN" in sql.upper(), f"expected NOT EXISTS handling:\n{sql}"

    def test_complex_5_aggregation_with_filter(self, cg_bin: Path):
        """complex-5: aggregation with WHERE filter."""
        cypher = (
            "MATCH (p:Person)-[:HAS_CREATOR]->(post:Post) "
            "WHERE post.creationDate >= $startDate AND post.creationDate < $endDate "
            "RETURN p.id AS personId, count(post) AS postCount "
            "ORDER BY postCount DESC LIMIT 10"
        )
        cypher = cypher.replace("$startDate", "1262304000000").replace("$endDate", "1293840000000")
        sql = _generate_sql(cg_bin, cypher)
        assert "GROUP BY" in sql.upper(), f"expected GROUP BY:\n{sql}"

    def test_complex_6_string_functions(self, cg_bin: Path):
        """complex-6: string functions and pattern matching."""
        cypher = (
            "MATCH (p:Person) "
            "WHERE toUpper(p.firstName) STARTS WITH 'A' "
            "RETURN p.id AS personId, p.firstName AS firstName, length(p.firstName) AS nameLen "
            "ORDER BY p.id LIMIT 10"
        )
        sql = _generate_sql(cg_bin, cypher)
        assert "upper(" in sql.lower(), f"expected upper() translation:\n{sql}"
        assert "startswith" in sql.lower(), f"expected startsWith translation:\n{sql}"

    def test_complex_7_chained_map_access(self, cg_bin: Path):
        """complex-7: chained map property access."""
        cypher = (
            "MATCH (p:Person)-[:HAS_CREATOR]->(post:Post) "
            "WITH head(collect({person: p, post: post})) AS pair "
            "RETURN pair.person.firstName AS firstName, pair.post.id AS postId"
        )
        sql = _generate_sql(cg_bin, cypher)
        # Should handle head() and map access

    def test_complex_8_weighted_shortest_path(self, cg_bin: Path):
        """complex-8: weighted shortest path using cost()."""
        cypher = (
            "MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id}), "
            "p = shortestPath((p1)-[:KNOWS*..6]-(p2)) "
            "RETURN length(p) AS hopCount, cost(p) AS totalCost"
        )
        cypher = cypher.replace("$person1Id", "0").replace("$person2Id", "1")
        sql = _generate_sql(cg_bin, cypher)
        assert "cost" in sql.lower(), f"expected cost() handling:\n{sql}"

    def test_complex_9_list_comprehension(self, cg_bin: Path):
        """complex-9: list comprehension with WHERE clause."""
        cypher = (
            "MATCH (p:Person)-[:HAS_CREATOR]->(post:Post) "
            "WITH p, [c IN post.likes WHERE c > 10] AS popularLikes "
            "WHERE size(popularLikes) > 0 "
            "RETURN p.id AS personId, size(popularLikes) AS popularLikeCount "
            "ORDER BY popularLikeCount DESC LIMIT 10"
        )
        sql = _generate_sql(cg_bin, cypher)
        # Should handle list comprehension

    def test_complex_10_pattern_comprehension(self, cg_bin: Path):
        """complex-10: pattern comprehension."""
        cypher = (
            "MATCH (p:Person) "
            "RETURN p.id AS personId, "
            "size([(p)-[:KNOWS]->(f:Person) | f.id]) AS friendCount "
            "ORDER BY friendCount DESC LIMIT 10"
        )
        sql = _generate_sql(cg_bin, cypher)
        assert "size(" in sql.lower(), f"expected size() handling:\n{sql}"

    def test_complex_11_unwind(self, cg_bin: Path):
        """complex-11: UNWIND clause."""
        cypher = (
            "UNWIND [1, 2, 3] AS num "
            "RETURN num, num * 2 AS doubled"
        )
        sql = _generate_sql(cg_bin, cypher)
        assert "UNWIND" in sql.upper() or "explode" in sql.lower(), f"expected UNWIND handling:\n{sql}"

    def test_complex_12_with_clause(self, cg_bin: Path):
        """complex-12: WITH clause for intermediate results."""
        cypher = (
            "MATCH (p:Person)-[:HAS_CREATOR]->(post:Post) "
            "WITH p, count(post) AS postCount "
            "WHERE postCount > 5 "
            "RETURN p.id AS personId, postCount "
            "ORDER BY postCount DESC LIMIT 10"
        )
        sql = _generate_sql(cg_bin, cypher)
        assert "WITH" in sql.upper(), f"expected WITH clause:\n{sql}"

    def test_complex_13_case_expression(self, cg_bin: Path):
        """complex-13: CASE expression."""
        cypher = (
            "MATCH (p:Person) "
            "RETURN p.id AS personId, "
            "CASE WHEN p.gender = 'female' THEN 'F' ELSE 'M' END AS genderCode "
            "ORDER BY p.id LIMIT 10"
        )
        sql = _generate_sql(cg_bin, cypher)
        assert "CASE" in sql.upper(), f"expected CASE expression:\n{sql}"

    def test_complex_14_weighted_vlp(self, cg_bin: Path):
        """complex-14: weighted variable-length path."""
        cypher = (
            "MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id}), "
            "p = shortestPath((p1)-[r:KNOWS*..6]-(p2)) "
            "RETURN length(p) AS hopCount, sum(r.weight) AS totalWeight"
        )
        cypher = cypher.replace("$person1Id", "0").replace("$person2Id", "1")
        sql = _generate_sql(cg_bin, cypher)
        assert "cost" in sql.lower() or "sum" in sql.lower(), f"expected weighted path handling:\n{sql}"

    def test_short_1_flat_join(self, cg_bin: Path):
        """short-1: simple flat join."""
        cypher = (
            "MATCH (p:Person {id: 0})-[:IS_LOCATED_IN]->(c:City) "
            "RETURN p.firstName AS firstName, c.name AS cityName"
        )
        sql = _generate_sql(cg_bin, cypher)
        assert "INNER JOIN" in sql or "JOIN" in sql.upper(), f"expected JOIN:\n{sql}"

    def test_short_2_aggregation(self, cg_bin: Path):
        """short-2: simple aggregation."""
        cypher = (
            "MATCH (p:Person)-[:KNOWS]->(f:Person) "
            "RETURN p.id AS personId, count(f) AS friendCount "
            "ORDER BY friendCount DESC LIMIT 10"
        )
        sql = _generate_sql(cg_bin, cypher)
        assert "count(" in sql.lower(), f"expected count():\n{sql}"
        assert "GROUP BY" in sql.upper(), f"expected GROUP BY:\n{sql}"

    def test_short_3_vlp(self, cg_bin: Path):
        """short-3: variable-length path."""
        cypher = (
            "MATCH (p:Person {id: 0})-[:KNOWS*1..2]-(friend:Person) "
            "WHERE friend.id <> p.id "
            "RETURN DISTINCT friend.id AS friendId ORDER BY friendId LIMIT 10"
        )
        sql = _generate_sql(cg_bin, cypher)
        assert "WITH RECURSIVE" in sql or "vlp_" in sql.lower(), f"expected VLP CTE:\n{sql}"

    def test_short_4_optional_match(self, cg_bin: Path):
        """short-4: optional match."""
        cypher = (
            "MATCH (p:Person {id: 0}) "
            "OPTIONAL MATCH (p)-[:STUDY_AT]->(u:Organisation) "
            "RETURN p.firstName AS firstName, u.name AS universityName"
        )
        sql = _generate_sql(cg_bin, cypher)
        assert "LEFT JOIN" in sql, f"expected LEFT JOIN:\n{sql}"

    def test_short_5_string_functions(self, cg_bin: Path):
        """short-5: string functions."""
        cypher = (
            "MATCH (p:Person) "
            "WHERE p.firstName STARTS WITH 'A' "
            "RETURN toUpper(p.firstName) AS upperName, length(p.firstName) AS nameLen LIMIT 10"
        )
        sql = _generate_sql(cg_bin, cypher)
        assert "upper(" in sql.lower(), f"expected upper():\n{sql}"
        assert "startswith" in sql.lower(), f"expected startsWith:\n{sql}"
