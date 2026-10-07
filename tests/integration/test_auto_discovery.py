"""
Integration tests for column auto-discovery (`auto_discover_columns`).

The server reads each discovering table's columns from ClickHouse when the
schema loads; `naming_convention` renames them, `exclude_columns` leaves some
out, and declared `property_mappings` are added on top.

Schema: schemas/examples/auto_discovery_demo.yaml over the brahmand.*_bench
tables (scripts/test/setup_all_test_data.sh).
"""

import os

import pytest
import requests
import yaml

CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:7475")
SCHEMA_PATH = "schemas/examples/auto_discovery_demo.yaml"


@pytest.fixture(scope="module")
def schema_name():
    """Load the auto-discovery demo schema through the API."""
    with open(SCHEMA_PATH, "r") as f:
        content = f.read()
    name = yaml.safe_load(content)["name"]
    response = requests.post(
        f"{CLICKGRAPH_URL}/schemas/load",
        json={"schema_name": name, "config_content": content, "validate_schema": True},
    )
    assert response.status_code == 200, f"Failed to load schema: {response.text}"
    return name


def query(schema_name, cypher):
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": f"USE {schema_name} {cypher}"},
    )
    assert response.status_code == 200, f"Query failed: {response.text}"
    return response.json()["results"]


def test_discovered_columns_are_properties(schema_name):
    """Every discovered column is a property, under its camelCase name."""
    rows = query(
        schema_name,
        "MATCH (u:User) WHERE u.userId = 1 "
        "RETURN u.userId, u.fullName, u.emailAddress, u.registrationDate, "
        "u.isActive, u.country",
    )
    assert rows == [
        {
            "u.userId": 1,
            "u.fullName": "Alice Smith",
            "u.emailAddress": "alice@example.com",
            "u.registrationDate": "2023-01-01",
            "u.isActive": 1,
            "u.country": "USA",
        }
    ]


def test_declared_mappings_are_added(schema_name):
    """Declared mappings sit alongside the discovered ones."""
    rows = query(schema_name, "MATCH (u:User) WHERE u.userId = 2 RETURN u.name, u.email")
    assert rows == [{"u.name": "Bob Johnson", "u.email": "bob@example.com"}]
    rows = query(schema_name, "MATCH (p:Post) RETURN p.body, p.content")
    assert rows and all(r["p.body"] == r["p.content"] for r in rows)


def test_relationship_columns_are_properties(schema_name):
    rows = query(
        schema_name,
        "MATCH (a:User)-[f:FOLLOWS]->(b:User) WHERE a.userId = 1 AND b.userId = 2 "
        "RETURN f.follow_date",
    )
    assert len(rows) == 1
    assert rows[0]["f.follow_date"]


def test_excluded_column_is_not_a_property(schema_name):
    """An excluded column is unknown, so NULL as in Cypher, and not part of the node."""
    rows = query(schema_name, "MATCH (u:User) WHERE u.userId = 1 RETURN u.city")
    assert rows == [{"u.city": None}]
    rows = query(schema_name, "MATCH (u:User) WHERE u.city IS NOT NULL RETURN u.userId")
    assert rows == []


def test_unknown_property_is_null(schema_name):
    """The discovered columns are all the properties; any other name is NULL."""
    rows = query(
        schema_name,
        "MATCH (a:User)-[f:FOLLOWS]->(b:User) WHERE a.userId = 1 AND b.userId = 2 "
        "RETURN a.nickname, f.weight",
    )
    assert rows == [{"a.nickname": None, "f.weight": None}]


def test_manual_schema_still_works():
    """Schemas without auto_discover_columns are unaffected."""
    with open("schemas/test/social_integration.yaml", "r") as f:
        content = f.read()
    name = yaml.safe_load(content).get("name", "social_integration")
    response = requests.post(
        f"{CLICKGRAPH_URL}/schemas/load",
        json={"schema_name": name, "config_content": content, "validate_schema": True},
    )
    assert response.status_code == 200
    rows = query(name, "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, u.email")
    assert len(rows) == 1
