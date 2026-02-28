"""Tests for schema analyzer."""

import pytest
from cg_schema.analyzer import (
    analyze,
    determine_pattern,
    generate_reason,
    classify_table_roles,
    extract_entity_stem,
    resolve_fk_targets,
    detect_edge_from_table_name,
    detect_polymorphic_labels,
)
from cg_schema.output import singularize, generate_yaml


class TestPatternDetection:
    """Test schema pattern detection (backward-compatible API)."""

    def test_standard_node_single_pk(self):
        result = determine_pattern(["user_id"], [], [])
        assert result == "standard_node"

    def test_standard_edge_multiple_fk(self):
        result = determine_pattern([], ["user_id", "post_id"], [])
        assert result == "standard_edge"

    def test_fk_node_single_pk_single_fk(self):
        """FK node with single PK and single FK - node with FK reference."""
        result = determine_pattern(["post_id"], ["user_id"], [])
        assert result == "fk_node"

    def test_denormalized_edge_origin_dest(self):
        columns = [
            {"name": "origin_user_id"},
            {"name": "dest_user_id"},
        ]
        result = determine_pattern([], [], columns)
        assert result == "denormalized_edge"

    def test_polymorphic_edge_type_column(self):
        columns = [{"name": "rel_type"}]
        result = determine_pattern([], ["from_id", "to_id"], columns)
        assert result == "polymorphic_edge"

    def test_composite_pk_junction_table(self):
        columns = [
            {"name": "user_id_1"},
            {"name": "user_id_2"},
            {"name": "since"},
        ]
        result = determine_pattern(["user_id_1", "user_id_2"], [], columns)
        assert result == "standard_edge"

    def test_flat_table_no_keys(self):
        result = determine_pattern([], [], [])
        assert result == "flat_table"

    def test_denormalized_edge_single_pk_with_origin_dest(self):
        columns = [
            {"name": "flight_id"},
            {"name": "flight_num"},
            {"name": "origin_code"},
            {"name": "origin_name"},
            {"name": "origin_country"},
            {"name": "dest_code"},
            {"name": "dest_name"},
            {"name": "dest_country"},
            {"name": "departure_time"},
            {"name": "arrival_time"},
        ]
        result = determine_pattern(["flight_id"], [], columns)
        assert result == "denormalized_edge"

    def test_standard_edge_junction_with_timestamp(self):
        columns = [
            {"name": "origin_airport"},
            {"name": "dest_airport"},
            {"name": "flight_date"},
        ]
        result = determine_pattern(["origin_airport", "dest_airport", "flight_date"], [], columns)
        assert result == "standard_edge"

    def test_polymorphic_edge_interactions(self):
        columns = [
            {"name": "from_id"},
            {"name": "to_id"},
            {"name": "interaction_type"},
            {"name": "from_type"},
            {"name": "to_type"},
            {"name": "timestamp"},
        ]
        result = determine_pattern([], ["from_id", "to_id"], columns)
        assert result == "polymorphic_edge"


class TestMultiRoleClassification:
    """Test the new multi-role classification system."""

    def test_comment_is_node_with_fk_edges(self):
        """LDBC Comment: node with creatorId and replyOfCommentId FK-edges."""
        roles = classify_table_roles(
            "Comment",
            pk_columns=["id"],
            fk_columns=["creatorId", "replyOfCommentId"],
            columns=[
                {"name": "id"},
                {"name": "creationDate"},
                {"name": "locationIP"},
                {"name": "browserUsed"},
                {"name": "content"},
                {"name": "length"},
                {"name": "creatorId"},
                {"name": "replyOfCommentId"},
            ],
        )
        assert roles["node_role"] == "standard_node"
        assert len(roles["edge_roles"]) == 2
        stems = {e["target_stem"] for e in roles["edge_roles"]}
        assert "creator" in stems
        assert "replyofcomment" in stems

    def test_post_is_node_with_fk_edge(self):
        """LDBC Post: node with creatorId FK-edge."""
        roles = classify_table_roles(
            "Post",
            pk_columns=["id"],
            fk_columns=["creatorId"],
            columns=[
                {"name": "id"},
                {"name": "imageFile"},
                {"name": "creationDate"},
                {"name": "content"},
                {"name": "length"},
                {"name": "creatorId"},
            ],
        )
        assert roles["node_role"] == "standard_node"
        assert len(roles["edge_roles"]) == 1
        assert roles["edge_roles"][0]["target_stem"] == "creator"

    def test_pure_junction_table_is_edge_only(self):
        """Person_knows_Person: pure edge, no node role."""
        roles = classify_table_roles(
            "Person_knows_Person",
            pk_columns=["Person1Id", "Person2Id"],
            fk_columns=[],
            columns=[
                {"name": "Person1Id"},
                {"name": "Person2Id"},
            ],
        )
        assert roles["node_role"] is None
        assert roles["edge_roles"] == []
        assert roles["pattern"] == "standard_edge"

    def test_thin_table_with_fks_is_edge(self):
        """Table with PK + 2 FKs but no attributes = edge."""
        roles = classify_table_roles(
            "user_role",
            pk_columns=["id"],
            fk_columns=["user_id", "role_id"],
            columns=[
                {"name": "id"},
                {"name": "user_id"},
                {"name": "role_id"},
            ],
        )
        assert roles["node_role"] is None
        assert roles["pattern"] == "fk_edge"

    def test_attribute_rich_table_with_fks_is_node(self):
        """Table with PK + 2 FKs + many attributes = node."""
        roles = classify_table_roles(
            "order",
            pk_columns=["order_id"],
            fk_columns=["customer_id", "product_id"],
            columns=[
                {"name": "order_id"},
                {"name": "customer_id"},
                {"name": "product_id"},
                {"name": "quantity"},
                {"name": "price"},
                {"name": "status"},
                {"name": "order_date"},
                {"name": "ship_date"},
            ],
        )
        assert roles["node_role"] == "standard_node"
        assert len(roles["edge_roles"]) == 2

    def test_files_with_folder_fk(self):
        """Files table: node with FK to folder."""
        roles = classify_table_roles(
            "files",
            pk_columns=["file_id"],
            fk_columns=["folder_id"],
            columns=[
                {"name": "file_id"},
                {"name": "name"},
                {"name": "folder_id"},
            ],
        )
        assert roles["node_role"] == "standard_node"
        assert len(roles["edge_roles"]) == 1
        assert roles["edge_roles"][0]["target_stem"] == "folder"


class TestEntityStemExtraction:
    """Test FK column -> entity stem extraction."""

    def test_snake_case(self):
        assert extract_entity_stem("user_id") == "user"
        assert extract_entity_stem("post_id") == "post"
        assert extract_entity_stem("folder_id") == "folder"

    def test_camel_case(self):
        assert extract_entity_stem("userId") == "user"
        assert extract_entity_stem("creatorId") == "creator"
        assert extract_entity_stem("person1Id") == "person1"
        assert extract_entity_stem("Person2Id") == "person2"

    def test_camel_case_uppercase(self):
        assert extract_entity_stem("userID") == "user"

    def test_key_suffix(self):
        assert extract_entity_stem("customer_key") == "customer"
        assert extract_entity_stem("product_sk") == "product"

    def test_compound_camel(self):
        assert extract_entity_stem("replyOfCommentId") == "replyofcomment"
        assert extract_entity_stem("replyOfPostId") == "replyofpost"


class TestCrossTableFkResolution:
    """Test the second-pass FK resolution."""

    def test_resolves_fk_to_node_table(self):
        suggestions = [
            {
                "table": "Person",
                "node_role": "standard_node",
                "pk_columns": ["personId"],
                "edge_roles": [],
            },
            {
                "table": "Comment",
                "node_role": "standard_node",
                "pk_columns": ["commentId"],
                "edge_roles": [
                    {"fk_column": "creatorId", "target_stem": "creator"},
                    {"fk_column": "replyOfCommentId", "target_stem": "replyofcomment"},
                ],
            },
        ]
        resolve_fk_targets(suggestions)

        comment_edges = suggestions[1]["edge_roles"]
        # "creator" won't match "person" by stem, but "replyofcomment" should match "comment"
        reply_edge = [e for e in comment_edges if e["fk_column"] == "replyOfCommentId"][0]
        assert reply_edge.get("to_table") == "Comment"

    def test_resolves_by_pk_stem(self):
        suggestions = [
            {
                "table": "users",
                "node_role": "standard_node",
                "pk_columns": ["user_id"],
                "edge_roles": [],
            },
            {
                "table": "posts",
                "node_role": "standard_node",
                "pk_columns": ["post_id"],
                "edge_roles": [
                    {"fk_column": "user_id", "target_stem": "user"},
                ],
            },
        ]
        resolve_fk_targets(suggestions)

        post_edge = suggestions[1]["edge_roles"][0]
        assert post_edge.get("to_table") == "users"
        assert post_edge.get("to_id") == "user_id"


class TestTableNameEdgeDetection:
    """Test Entity_verb_Entity table name pattern detection."""

    def test_person_knows_person(self):
        result = detect_edge_from_table_name(
            "Person_knows_Person", {"Person", "Post", "Comment"}
        )
        assert result is not None
        assert result["from_node"] == "Person"
        assert result["to_node"] == "Person"
        assert result["type"] == "KNOWS"

    def test_forum_container_of_post(self):
        result = detect_edge_from_table_name(
            "Forum_containerOf_Post", {"Forum", "Post", "Person"}
        )
        assert result is not None
        assert result["from_node"] == "Forum"
        assert result["to_node"] == "Post"
        assert result["type"] == "CONTAINER_OF"

    def test_person_has_interest_tag(self):
        result = detect_edge_from_table_name(
            "Person_hasInterest_Tag", {"Person", "Tag", "Forum"}
        )
        assert result is not None
        assert result["from_node"] == "Person"
        assert result["to_node"] == "Tag"
        assert result["type"] == "HAS_INTEREST"

    def test_no_match_returns_none(self):
        result = detect_edge_from_table_name(
            "random_table", {"Person", "Post"}
        )
        assert result is None

    def test_two_word_entity(self):
        """Tables with underscore in entity names."""
        result = detect_edge_from_table_name(
            "Tag_hasType_TagClass", {"Tag", "TagClass"}
        )
        assert result is not None
        assert result["from_node"] == "Tag"
        assert result["to_node"] == "TagClass"
        assert result["type"] == "HAS_TYPE"

    def test_case_insensitive_matching(self):
        result = detect_edge_from_table_name(
            "person_likes_Post", {"Person", "Post"}
        )
        assert result is not None
        assert result["from_node"] == "Person"
        assert result["to_node"] == "Post"


class TestPolymorphicLabelDetection:
    """Test polymorphic sub-label detection from type columns."""

    def test_place_with_type_column(self):
        columns = [
            {"name": "id"},
            {"name": "name"},
            {"name": "type"},
        ]
        sample = [
            {"id": 1, "name": "New York", "type": "City"},
            {"id": 2, "name": "USA", "type": "Country"},
            {"id": 3, "name": "Europe", "type": "Continent"},
        ]
        result = detect_polymorphic_labels("Place", columns, sample)
        assert result is not None
        assert len(result) == 3
        labels = {r["label"] for r in result}
        assert labels == {"City", "Country", "Continent"}
        # Check filters
        city = [r for r in result if r["label"] == "City"][0]
        assert city["filter"] == "type = 'City'"

    def test_organisation_with_type(self):
        columns = [{"name": "id"}, {"name": "name"}, {"name": "type"}]
        sample = [
            {"id": 1, "name": "MIT", "type": "University"},
            {"id": 2, "name": "Google", "type": "Company"},
        ]
        result = detect_polymorphic_labels("Organisation", columns, sample)
        assert result is not None
        labels = {r["label"] for r in result}
        assert labels == {"Company", "University"}

    def test_no_type_column(self):
        columns = [{"name": "id"}, {"name": "name"}]
        sample = [{"id": 1, "name": "Test"}]
        result = detect_polymorphic_labels("test", columns, sample)
        assert result is None

    def test_empty_sample(self):
        columns = [{"name": "id"}, {"name": "type"}]
        result = detect_polymorphic_labels("test", columns, [])
        assert result is None

    def test_single_value_not_polymorphic(self):
        """Only one distinct type value -> not polymorphic."""
        columns = [{"name": "id"}, {"name": "type"}]
        sample = [{"id": 1, "type": "A"}, {"id": 2, "type": "A"}]
        result = detect_polymorphic_labels("test", columns, sample)
        assert result is None


class TestReasonGeneration:
    """Test human-readable reason generation."""

    def test_standard_node_reason(self):
        reason = generate_reason("node", "standard_node", ["user_id"], [])
        assert "primary key" in reason.lower() or "node" in reason.lower()

    def test_standard_edge_reason(self):
        reason = generate_reason("edge", "standard_edge", [], ["user_id", "post_id"])
        assert "edge" in reason.lower()

    def test_dual_role_reason(self):
        reason = generate_reason(
            "node", "fk_node", ["id"], ["creatorId"],
            node_role="standard_node",
            edge_roles=[{"target_stem": "creator", "fk_column": "creatorId"}],
        )
        assert "node" in reason.lower()
        assert "fk-edge" in reason.lower()
        assert "creator" in reason.lower()


class TestRealWorldSchemas:
    """Test against real-world schema patterns."""

    def test_social_users(self):
        tables = [{
            "name": "users",
            "columns": [
                {"name": "user_id", "is_primary_key": True},
                {"name": "username", "is_primary_key": False},
                {"name": "age", "is_primary_key": False},
                {"name": "city", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_node"
        assert suggestions[0]["node_role"] == "standard_node"

    def test_social_follows(self):
        tables = [{
            "name": "follows",
            "columns": [
                {"name": "follower_id", "is_primary_key": True},
                {"name": "followed_id", "is_primary_key": True},
                {"name": "follow_date", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_edge"
        assert suggestions[0]["node_role"] is None

    def test_ldbc_person(self):
        tables = [{
            "name": "Person",
            "columns": [
                {"name": "id", "is_primary_key": True},
                {"name": "firstName", "is_primary_key": False},
                {"name": "lastName", "is_primary_key": False},
                {"name": "gender", "is_primary_key": False},
                {"name": "birthday", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_node"

    def test_ldbc_person_knows_person(self):
        tables = [{
            "name": "Person_knows_Person",
            "columns": [
                {"name": "Person1Id", "is_primary_key": True},
                {"name": "Person2Id", "is_primary_key": True},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_edge"

    def test_ldbc_comment_dual_role(self):
        """LDBC Comment: node + FK-edges (the key test for dual-role)."""
        tables = [{
            "name": "Comment",
            "columns": [
                {"name": "id", "is_primary_key": True},
                {"name": "creationDate", "is_primary_key": False},
                {"name": "locationIP", "is_primary_key": False},
                {"name": "browserUsed", "is_primary_key": False},
                {"name": "content", "is_primary_key": False},
                {"name": "length", "is_primary_key": False},
                {"name": "creatorId", "is_primary_key": False},
                {"name": "replyOfCommentId", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        s = suggestions[0]
        assert s["node_role"] == "standard_node", f"Comment should be a node, got: {s}"
        assert len(s["edge_roles"]) == 2
        fk_cols = {e["fk_column"] for e in s["edge_roles"]}
        assert fk_cols == {"creatorId", "replyOfCommentId"}

    def test_ldbc_post_dual_role(self):
        """LDBC Post: node + FK-edge to creator."""
        tables = [{
            "name": "Post",
            "columns": [
                {"name": "id", "is_primary_key": True},
                {"name": "imageFile", "is_primary_key": False},
                {"name": "creationDate", "is_primary_key": False},
                {"name": "content", "is_primary_key": False},
                {"name": "length", "is_primary_key": False},
                {"name": "creatorId", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        s = suggestions[0]
        assert s["node_role"] == "standard_node"
        assert len(s["edge_roles"]) == 1
        assert s["edge_roles"][0]["fk_column"] == "creatorId"

    def test_travel_flights(self):
        tables = [{
            "name": "flights",
            "columns": [
                {"name": "origin_airport", "is_primary_key": True},
                {"name": "dest_airport", "is_primary_key": True},
                {"name": "flight_date", "is_primary_key": True},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_edge"

    def test_travel_flights_denorm(self):
        tables = [{
            "name": "flights_denorm",
            "columns": [
                {"name": "flight_id", "is_primary_key": True},
                {"name": "origin_code", "is_primary_key": False},
                {"name": "origin_name", "is_primary_key": False},
                {"name": "dest_code", "is_primary_key": False},
                {"name": "dest_name", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "denormalized_edge"

    def test_brahmand_interactions(self):
        tables = [{
            "name": "interactions",
            "columns": [
                {"name": "from_id", "is_primary_key": False},
                {"name": "to_id", "is_primary_key": False},
                {"name": "interaction_type", "is_primary_key": False},
                {"name": "from_type", "is_primary_key": False},
                {"name": "to_type", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "polymorphic_edge"

    def test_brahmand_groups(self):
        tables = [{
            "name": "groups",
            "columns": [
                {"name": "group_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_node"

    def test_brahmand_memberships(self):
        tables = [{
            "name": "memberships",
            "columns": [
                {"name": "user_id", "is_primary_key": True},
                {"name": "group_id", "is_primary_key": True},
                {"name": "joined_at", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_edge"

    def test_community_interactions(self):
        tables = [{
            "name": "interactions",
            "columns": [
                {"name": "from_member_id", "is_primary_key": True},
                {"name": "to_member_id", "is_primary_key": True},
                {"name": "interaction_type", "is_primary_key": True},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_edge"

    def test_data_security_users(self):
        tables = [{
            "name": "ds_users",
            "columns": [
                {"name": "user_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_node"

    def test_data_security_permissions(self):
        tables = [{
            "name": "ds_permissions",
            "columns": [
                {"name": "subject_id", "is_primary_key": True},
                {"name": "object_id", "is_primary_key": True},
                {"name": "permission", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_edge"

    def test_filesystem_files(self):
        tables = [{
            "name": "files",
            "columns": [
                {"name": "file_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
                {"name": "folder_id", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["node_role"] == "standard_node"
        assert len(suggestions[0]["edge_roles"]) == 1

    def test_filesystem_folders(self):
        tables = [{
            "name": "folders",
            "columns": [
                {"name": "folder_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
                {"name": "parent_folder_id", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["node_role"] == "standard_node"
        assert len(suggestions[0]["edge_roles"]) == 1

    def test_lineage_file_lineage(self):
        tables = [{
            "name": "file_lineage",
            "columns": [
                {"name": "source_file_id", "is_primary_key": True},
                {"name": "target_file_id", "is_primary_key": True},
                {"name": "transform", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_edge"

    def test_security_conn(self):
        tables = [{
            "name": "conn",
            "columns": [
                {"name": "id_orig_h", "is_primary_key": True},
                {"name": "id_resp_h", "is_primary_key": True},
                {"name": "id_orig_p", "is_primary_key": False},
                {"name": "id_resp_p", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_edge"

    def test_db_multi_tenant_posts(self):
        tables = [{
            "name": "posts",
            "columns": [
                {"name": "post_id", "is_primary_key": True},
                {"name": "user_id", "is_primary_key": False},
                {"name": "content", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["node_role"] == "standard_node"
        assert len(suggestions[0]["edge_roles"]) == 1

    def test_db_multi_tenant_user_follows(self):
        tables = [{
            "name": "user_follows",
            "columns": [
                {"name": "follower_id", "is_primary_key": True},
                {"name": "followed_id", "is_primary_key": True},
                {"name": "tenant_id", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_edge"

    def test_db_polymorphic_interactions(self):
        tables = [{
            "name": "interactions",
            "columns": [
                {"name": "from_id", "is_primary_key": False},
                {"name": "to_id", "is_primary_key": False},
                {"name": "type", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "polymorphic_edge"


class TestFullLdbcDatabase:
    """Test a realistic LDBC-like database with multiple tables.

    Validates cross-table FK resolution and table name edge detection
    working together.
    """

    def _make_ldbc_tables(self):
        return [
            {
                "name": "Person",
                "columns": [
                    {"name": "id", "is_primary_key": True},
                    {"name": "firstName", "is_primary_key": False},
                    {"name": "lastName", "is_primary_key": False},
                    {"name": "gender", "is_primary_key": False},
                ],
            },
            {
                "name": "Post",
                "columns": [
                    {"name": "id", "is_primary_key": True},
                    {"name": "content", "is_primary_key": False},
                    {"name": "creationDate", "is_primary_key": False},
                    {"name": "creatorId", "is_primary_key": False},
                ],
            },
            {
                "name": "Comment",
                "columns": [
                    {"name": "id", "is_primary_key": True},
                    {"name": "content", "is_primary_key": False},
                    {"name": "creationDate", "is_primary_key": False},
                    {"name": "creatorId", "is_primary_key": False},
                    {"name": "replyOfPostId", "is_primary_key": False},
                    {"name": "replyOfCommentId", "is_primary_key": False},
                ],
            },
            {
                "name": "Tag",
                "columns": [
                    {"name": "id", "is_primary_key": True},
                    {"name": "name", "is_primary_key": False},
                ],
            },
            {
                "name": "Person_knows_Person",
                "columns": [
                    {"name": "Person1Id", "is_primary_key": True},
                    {"name": "Person2Id", "is_primary_key": True},
                    {"name": "creationDate", "is_primary_key": False},
                ],
            },
            {
                "name": "Person_hasInterest_Tag",
                "columns": [
                    {"name": "PersonId", "is_primary_key": True},
                    {"name": "TagId", "is_primary_key": True},
                ],
            },
        ]

    def test_node_tables_detected(self):
        suggestions = analyze(self._make_ldbc_tables())
        by_table = {s["table"]: s for s in suggestions}

        assert by_table["Person"]["node_role"] == "standard_node"
        assert by_table["Post"]["node_role"] == "standard_node"
        assert by_table["Comment"]["node_role"] == "standard_node"
        assert by_table["Tag"]["node_role"] == "standard_node"

    def test_edge_tables_detected(self):
        suggestions = analyze(self._make_ldbc_tables())
        by_table = {s["table"]: s for s in suggestions}

        assert by_table["Person_knows_Person"]["node_role"] is None
        assert by_table["Person_knows_Person"]["pattern"] == "standard_edge"
        assert by_table["Person_hasInterest_Tag"]["node_role"] is None

    def test_table_name_edge_detection(self):
        suggestions = analyze(self._make_ldbc_tables())
        by_table = {s["table"]: s for s in suggestions}

        knows = by_table["Person_knows_Person"]
        assert knows.get("name_edge_info") is not None
        assert knows["name_edge_info"]["from_node"] == "Person"
        assert knows["name_edge_info"]["to_node"] == "Person"
        assert knows["name_edge_info"]["type"] == "KNOWS"

        interest = by_table["Person_hasInterest_Tag"]
        assert interest.get("name_edge_info") is not None
        assert interest["name_edge_info"]["from_node"] == "Person"
        assert interest["name_edge_info"]["to_node"] == "Tag"

    def test_fk_resolution_across_tables(self):
        suggestions = analyze(self._make_ldbc_tables())
        by_table = {s["table"]: s for s in suggestions}

        comment = by_table["Comment"]
        # replyOfCommentId should resolve to Comment table
        reply_edge = [e for e in comment["edge_roles"] if e["fk_column"] == "replyOfCommentId"][0]
        assert reply_edge.get("to_table") == "Comment"

        # replyOfPostId should resolve to Post table
        post_edge = [e for e in comment["edge_roles"] if e["fk_column"] == "replyOfPostId"][0]
        assert post_edge.get("to_table") == "Post"


class TestPolymorphicIntegration:
    """Test polymorphic sub-label detection through the analyze() pipeline."""

    def test_place_polymorphic_labels(self):
        tables = [{
            "name": "Place",
            "columns": [
                {"name": "id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
                {"name": "type", "is_primary_key": False},
            ],
            "sample": [
                {"id": 1, "name": "New York", "type": "City"},
                {"id": 2, "name": "USA", "type": "Country"},
                {"id": 3, "name": "Europe", "type": "Continent"},
            ],
        }]
        suggestions = analyze(tables)
        s = suggestions[0]
        assert s["node_role"] == "standard_node"
        assert s["polymorphic_labels"] is not None
        labels = {pl["label"] for pl in s["polymorphic_labels"]}
        assert labels == {"City", "Country", "Continent"}


class TestSingularize:
    """Test improved singularization."""

    def test_regular_plurals(self):
        assert singularize("users") == "user"
        assert singularize("posts") == "post"
        assert singularize("tags") == "tag"

    def test_ies_plural(self):
        assert singularize("categories") == "category"
        assert singularize("companies") == "company"

    def test_es_plural(self):
        assert singularize("addresses") == "address"
        assert singularize("boxes") == "box"
        assert singularize("statuses") == "status"

    def test_irregular_plurals(self):
        assert singularize("people") == "person"
        assert singularize("People") == "Person"
        assert singularize("analyses") == "analysis"
        assert singularize("indices") == "index"
        assert singularize("vertices") == "vertex"

    def test_no_strip_suffixes(self):
        assert singularize("status") == "status"
        assert singularize("analysis") == "analysis"

    def test_already_singular(self):
        assert singularize("person") == "person"
        assert singularize("data") == "data"
        assert singularize("series") == "series"

    def test_capitalized(self):
        assert singularize("Users") == "User"
        assert singularize("Categories") == "Category"


class TestYamlGeneration:
    """Test YAML output generation."""

    def test_dual_role_generates_node_and_edges(self):
        """A dual-role table should produce both a node and FK-edge entries."""
        tables = [{
            "name": "Comment",
            "columns": [
                {"name": "id", "is_primary_key": True},
                {"name": "content", "is_primary_key": False},
                {"name": "creatorId", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        yaml_str = generate_yaml(tables, suggestions, database="ldbc")

        import yaml as yaml_lib
        schema = yaml_lib.safe_load(yaml_str)
        gs = schema["graph_schema"]

        # Should have a node
        assert "nodes" in gs
        node_labels = [n["label"] for n in gs["nodes"]]
        assert "Comment" in node_labels

        # Should have an FK-edge
        assert "edges" in gs
        assert len(gs["edges"]) >= 1
        edge = gs["edges"][0]
        assert edge["from_node"] == "Comment"
        assert edge["to_id"] == "creatorId"

    def test_pure_edge_yaml(self):
        tables = [{
            "name": "follows",
            "columns": [
                {"name": "follower_id", "is_primary_key": True},
                {"name": "followed_id", "is_primary_key": True},
            ]
        }]
        suggestions = analyze(tables)
        yaml_str = generate_yaml(tables, suggestions, database="social")

        import yaml as yaml_lib
        schema = yaml_lib.safe_load(yaml_str)
        gs = schema["graph_schema"]

        assert "edges" in gs
        assert "nodes" not in gs

    def test_polymorphic_sub_labels_in_yaml(self):
        tables = [{
            "name": "Place",
            "columns": [
                {"name": "id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
                {"name": "type", "is_primary_key": False},
            ],
            "sample": [
                {"id": 1, "name": "NYC", "type": "City"},
                {"id": 2, "name": "USA", "type": "Country"},
            ],
        }]
        suggestions = analyze(tables)
        yaml_str = generate_yaml(tables, suggestions, database="ldbc")

        import yaml as yaml_lib
        schema = yaml_lib.safe_load(yaml_str)
        gs = schema["graph_schema"]

        node_labels = [n["label"] for n in gs["nodes"]]
        assert "Place" in node_labels
        assert "City" in node_labels
        assert "Country" in node_labels

        # Sub-labels should have filter
        city_node = [n for n in gs["nodes"] if n["label"] == "City"][0]
        assert city_node["filter"] == "type = 'City'"
        assert city_node["database"] == "ldbc"
        assert city_node["table"] == "Place"

    def test_table_name_edge_in_yaml(self):
        """Edge tables with Entity_verb_Entity names get resolved endpoints."""
        tables = [
            {
                "name": "Person",
                "columns": [
                    {"name": "id", "is_primary_key": True},
                    {"name": "name", "is_primary_key": False},
                ],
            },
            {
                "name": "Tag",
                "columns": [
                    {"name": "id", "is_primary_key": True},
                    {"name": "name", "is_primary_key": False},
                ],
            },
            {
                "name": "Person_hasInterest_Tag",
                "columns": [
                    {"name": "PersonId", "is_primary_key": True},
                    {"name": "TagId", "is_primary_key": True},
                ],
            },
        ]
        suggestions = analyze(tables)
        yaml_str = generate_yaml(tables, suggestions, database="ldbc")

        import yaml as yaml_lib
        schema = yaml_lib.safe_load(yaml_str)
        gs = schema["graph_schema"]

        edges = gs.get("edges", [])
        interest_edge = [e for e in edges if e["table"] == "Person_hasInterest_Tag"]
        assert len(interest_edge) == 1
        e = interest_edge[0]
        assert e["from_node"] == "Person"
        assert e["to_node"] == "Tag"
        assert e["type"] == "HAS_INTEREST"


class TestColumnNamingPatterns:
    """Test column naming patterns for FK/PK detection."""

    def test_camel_case_pk(self):
        tables = [{
            "name": "test",
            "columns": [{"name": "userId", "is_primary_key": True}],
        }]
        suggestions = analyze(tables)
        assert "userId" in suggestions[0]["pk_columns"]

    def test_camel_case_fk(self):
        tables = [{
            "name": "test",
            "columns": [{"name": "creatorId", "is_primary_key": False}],
        }]
        suggestions = analyze(tables)
        assert "creatorId" in suggestions[0]["fk_columns"]

    def test_snake_case_fk(self):
        tables = [{
            "name": "test",
            "columns": [{"name": "user_id", "is_primary_key": False}],
        }]
        suggestions = analyze(tables)
        assert "user_id" in suggestions[0]["fk_columns"]

    def test_uppercase_id(self):
        tables = [{
            "name": "test",
            "columns": [{"name": "ID", "is_primary_key": True}],
        }]
        suggestions = analyze(tables)
        assert "ID" in suggestions[0]["pk_columns"]

    def test_composite_key_naming(self):
        result = determine_pattern(["userId", "postId"], [], [])
        assert result == "standard_edge"

        result = determine_pattern(["user_id", "post_id"], [], [])
        assert result == "standard_edge"

    def test_prefixed_id_columns(self):
        result = determine_pattern(["id_user", "id_post"], [], [])
        assert result == "standard_edge"


class TestValueAnalysis:
    """Test value-based analysis from sample data."""

    def test_email_detection(self):
        from cg_schema.analyzer import analyze_sample_values
        sample = [
            {"email": "user@example.com", "name": "John"},
            {"email": "jane@example.com", "name": "Jane"},
        ]
        result = analyze_sample_values(sample)
        assert result["enabled"] is True
        assert any(p["type"] == "email" for p in result["patterns"])

    def test_url_detection(self):
        from cg_schema.analyzer import analyze_sample_values
        sample = [{"url": "https://example.com/page1", "name": "Page 1"}]
        result = analyze_sample_values(sample)
        assert result["enabled"] is True
        assert any(p["type"] == "url" for p in result["patterns"])

    def test_uuid_detection(self):
        from cg_schema.analyzer import analyze_sample_values
        sample = [{"id": "550e8400-e29b-41d4-a716-446655440000", "name": "Test"}]
        result = analyze_sample_values(sample)
        assert result["enabled"] is True
        assert any(p["type"] == "uuid" for p in result["patterns"])

    def test_empty_sample(self):
        from cg_schema.analyzer import analyze_sample_values
        result = analyze_sample_values([])
        assert result["enabled"] is False


# ============================================================================
# Real-world public schema tests
# ============================================================================


class TestSSBStarSchema:
    """Star Schema Benchmark (SSB) — ClickHouse's official DWH benchmark.

    Tests the fact + dimension table pattern common in analytics warehouses.
    SSB uses non-standard column naming (C_CUSTKEY, LO_CUSTKEY, etc.) which
    is invisible to _id/_key heuristics without is_primary_key metadata.
    Source: https://clickhouse.com/docs/getting-started/example-datasets/star-schema
    """

    def _make_ssb_tables(self):
        return [
            {
                "name": "customer",
                "columns": [
                    {"name": "C_CUSTKEY", "is_primary_key": True},
                    {"name": "C_NAME", "is_primary_key": False},
                    {"name": "C_ADDRESS", "is_primary_key": False},
                    {"name": "C_CITY", "is_primary_key": False},
                    {"name": "C_NATION", "is_primary_key": False},
                    {"name": "C_REGION", "is_primary_key": False},
                    {"name": "C_PHONE", "is_primary_key": False},
                    {"name": "C_MKTSEGMENT", "is_primary_key": False},
                ],
            },
            {
                "name": "supplier",
                "columns": [
                    {"name": "S_SUPPKEY", "is_primary_key": True},
                    {"name": "S_NAME", "is_primary_key": False},
                    {"name": "S_ADDRESS", "is_primary_key": False},
                    {"name": "S_CITY", "is_primary_key": False},
                    {"name": "S_NATION", "is_primary_key": False},
                    {"name": "S_REGION", "is_primary_key": False},
                    {"name": "S_PHONE", "is_primary_key": False},
                ],
            },
            {
                "name": "part",
                "columns": [
                    {"name": "P_PARTKEY", "is_primary_key": True},
                    {"name": "P_NAME", "is_primary_key": False},
                    {"name": "P_MFGR", "is_primary_key": False},
                    {"name": "P_CATEGORY", "is_primary_key": False},
                    {"name": "P_BRAND", "is_primary_key": False},
                    {"name": "P_COLOR", "is_primary_key": False},
                    {"name": "P_TYPE", "is_primary_key": False},
                    {"name": "P_SIZE", "is_primary_key": False},
                ],
            },
            {
                "name": "lineorder",
                "columns": [
                    {"name": "LO_ORDERKEY", "is_primary_key": True},
                    {"name": "LO_LINENUMBER", "is_primary_key": True},
                    {"name": "LO_CUSTKEY", "is_primary_key": False},
                    {"name": "LO_PARTKEY", "is_primary_key": False},
                    {"name": "LO_SUPPKEY", "is_primary_key": False},
                    {"name": "LO_ORDERDATE", "is_primary_key": False},
                    {"name": "LO_QUANTITY", "is_primary_key": False},
                    {"name": "LO_EXTENDEDPRICE", "is_primary_key": False},
                    {"name": "LO_REVENUE", "is_primary_key": False},
                    {"name": "LO_SUPPLYCOST", "is_primary_key": False},
                    {"name": "LO_DISCOUNT", "is_primary_key": False},
                    {"name": "LO_TAX", "is_primary_key": False},
                    {"name": "LO_SHIPMODE", "is_primary_key": False},
                ],
            },
        ]

    def test_dimension_tables_are_nodes(self):
        """SSB dimension tables should all be nodes."""
        suggestions = analyze(self._make_ssb_tables())
        by_table = {s["table"]: s for s in suggestions}
        assert by_table["customer"]["node_role"] == "standard_node"
        assert by_table["supplier"]["node_role"] == "standard_node"
        assert by_table["part"]["node_role"] == "standard_node"

    def test_fact_table_is_edge(self):
        """Lineorder has composite PK → edge. Non-standard _KEY cols aren't
        detected as FKs since they lack _id/_key suffix in lowercase.
        ClickHouse metadata (is_primary_key) makes the PKs work."""
        suggestions = analyze(self._make_ssb_tables())
        by_table = {s["table"]: s for s in suggestions}
        lo = by_table["lineorder"]
        assert lo["node_role"] is None
        assert lo["pattern"] == "standard_edge"


class TestNYCTaxi:
    """NYC Taxi dataset — single denormalized flat table.

    No FK columns, no composite PK. Just a wide event table.
    Source: https://clickhouse.com/docs/getting-started/example-datasets/nyc-taxi
    """

    def test_trips_is_flat_or_node(self):
        tables = [{
            "name": "trips",
            "columns": [
                {"name": "trip_id", "is_primary_key": True},
                {"name": "pickup_datetime", "is_primary_key": False},
                {"name": "dropoff_datetime", "is_primary_key": False},
                {"name": "pickup_longitude", "is_primary_key": False},
                {"name": "pickup_latitude", "is_primary_key": False},
                {"name": "dropoff_longitude", "is_primary_key": False},
                {"name": "dropoff_latitude", "is_primary_key": False},
                {"name": "passenger_count", "is_primary_key": False},
                {"name": "trip_distance", "is_primary_key": False},
                {"name": "fare_amount", "is_primary_key": False},
                {"name": "tip_amount", "is_primary_key": False},
                {"name": "total_amount", "is_primary_key": False},
                {"name": "payment_type", "is_primary_key": False},
            ],
        }]
        suggestions = analyze(tables)
        s = suggestions[0]
        # Single PK + no FKs → standard_node
        assert s["node_role"] == "standard_node"
        assert s["pattern"] == "standard_node"
        assert len(s["edge_roles"]) == 0


class TestGitHubEvents:
    """GitHub repository data — commits, file_changes, line_changes.

    Tests tables with hash-based PKs and non-standard FK naming.
    Source: https://clickhouse.com/docs/getting-started/example-datasets/github
    """

    def _make_github_tables(self):
        return [
            {
                "name": "commits",
                "columns": [
                    {"name": "hash", "is_primary_key": True},
                    {"name": "author", "is_primary_key": False},
                    {"name": "time", "is_primary_key": False},
                    {"name": "message", "is_primary_key": False},
                    {"name": "files_added", "is_primary_key": False},
                    {"name": "files_deleted", "is_primary_key": False},
                    {"name": "files_renamed", "is_primary_key": False},
                    {"name": "files_modified", "is_primary_key": False},
                ],
            },
            {
                "name": "file_changes",
                "columns": [
                    {"name": "change_type", "is_primary_key": False},
                    {"name": "path", "is_primary_key": False},
                    {"name": "old_path", "is_primary_key": False},
                    {"name": "file_extension", "is_primary_key": False},
                    {"name": "lines_added", "is_primary_key": False},
                    {"name": "lines_deleted", "is_primary_key": False},
                    {"name": "commit_hash", "is_primary_key": False},
                ],
            },
        ]

    def test_commits_is_node(self):
        suggestions = analyze(self._make_github_tables())
        by_table = {s["table"]: s for s in suggestions}
        assert by_table["commits"]["node_role"] == "standard_node"

    def test_file_changes_no_pk(self):
        """file_changes has no PK and no conventional FK → flat_table."""
        suggestions = analyze(self._make_github_tables())
        by_table = {s["table"]: s for s in suggestions}
        assert by_table["file_changes"]["pattern"] == "flat_table"


class TestOpenTelemetry:
    """OpenTelemetry observability schema — logs and traces.

    Tests tables with non-conventional naming and Map types.
    Source: https://github.com/open-telemetry/opentelemetry-collector-contrib
    """

    def test_otel_traces(self):
        tables = [{
            "name": "otel_traces",
            "columns": [
                {"name": "Timestamp", "is_primary_key": False},
                {"name": "TraceId", "is_primary_key": False},
                {"name": "SpanId", "is_primary_key": False},
                {"name": "ParentSpanId", "is_primary_key": False},
                {"name": "SpanName", "is_primary_key": False},
                {"name": "SpanKind", "is_primary_key": False},
                {"name": "ServiceName", "is_primary_key": False},
                {"name": "Duration", "is_primary_key": False},
                {"name": "StatusCode", "is_primary_key": False},
                {"name": "StatusMessage", "is_primary_key": False},
            ],
        }]
        suggestions = analyze(tables)
        s = suggestions[0]
        # No PK, no FK-like columns → flat_table
        assert s["pattern"] == "flat_table"

    def test_otel_logs(self):
        tables = [{
            "name": "otel_logs",
            "columns": [
                {"name": "Timestamp", "is_primary_key": False},
                {"name": "TraceId", "is_primary_key": False},
                {"name": "SpanId", "is_primary_key": False},
                {"name": "SeverityText", "is_primary_key": False},
                {"name": "SeverityNumber", "is_primary_key": False},
                {"name": "ServiceName", "is_primary_key": False},
                {"name": "Body", "is_primary_key": False},
            ],
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "flat_table"


class TestNYPDComplaint:
    """NYPD Complaint Data — single wide event/dimension table.

    Has location_type column which triggers polymorphic detection.
    Source: https://clickhouse.com/docs/getting-started/example-datasets/nypd_complaint_data
    """

    def test_complaint_table(self):
        tables = [{
            "name": "NYPD_Complaint",
            "columns": [
                {"name": "complaint_number", "is_primary_key": True},
                {"name": "precinct", "is_primary_key": False},
                {"name": "borough", "is_primary_key": False},
                {"name": "complaint_begin", "is_primary_key": False},
                {"name": "complaint_end", "is_primary_key": False},
                {"name": "offense_code", "is_primary_key": False},
                {"name": "offense_level", "is_primary_key": False},
                {"name": "offense_description", "is_primary_key": False},
                {"name": "location_type", "is_primary_key": False},
                {"name": "suspect_age_group", "is_primary_key": False},
                {"name": "victim_age_group", "is_primary_key": False},
                {"name": "Latitude", "is_primary_key": False},
                {"name": "Longitude", "is_primary_key": False},
            ],
        }]
        suggestions = analyze(tables)
        s = suggestions[0]
        # Single PK, no FKs → standard_node
        assert s["node_role"] == "standard_node"
        assert s["pattern"] == "standard_node"


class TestECommerce:
    """E-commerce schema — tests bridge/line-item tables and hub nodes."""

    def _make_ecommerce_tables(self):
        return [
            {
                "name": "customers",
                "columns": [
                    {"name": "customer_id", "is_primary_key": True},
                    {"name": "name", "is_primary_key": False},
                    {"name": "email", "is_primary_key": False},
                    {"name": "city", "is_primary_key": False},
                ],
            },
            {
                "name": "products",
                "columns": [
                    {"name": "product_id", "is_primary_key": True},
                    {"name": "name", "is_primary_key": False},
                    {"name": "category", "is_primary_key": False},
                    {"name": "price", "is_primary_key": False},
                ],
            },
            {
                "name": "orders",
                "columns": [
                    {"name": "order_id", "is_primary_key": True},
                    {"name": "customer_id", "is_primary_key": False},
                    {"name": "order_date", "is_primary_key": False},
                    {"name": "status", "is_primary_key": False},
                    {"name": "total_amount", "is_primary_key": False},
                ],
            },
            {
                "name": "order_items",
                "columns": [
                    {"name": "order_id", "is_primary_key": True},
                    {"name": "product_id", "is_primary_key": True},
                    {"name": "quantity", "is_primary_key": False},
                    {"name": "unit_price", "is_primary_key": False},
                    {"name": "discount_pct", "is_primary_key": False},
                    {"name": "line_total", "is_primary_key": False},
                ],
            },
            {
                "name": "reviews",
                "columns": [
                    {"name": "review_id", "is_primary_key": True},
                    {"name": "product_id", "is_primary_key": False},
                    {"name": "customer_id", "is_primary_key": False},
                    {"name": "rating", "is_primary_key": False},
                    {"name": "title", "is_primary_key": False},
                    {"name": "body", "is_primary_key": False},
                    {"name": "created_at", "is_primary_key": False},
                ],
            },
        ]

    def test_node_tables(self):
        suggestions = analyze(self._make_ecommerce_tables())
        by_table = {s["table"]: s for s in suggestions}
        assert by_table["customers"]["node_role"] == "standard_node"
        assert by_table["products"]["node_role"] == "standard_node"

    def test_orders_is_node_with_fk_edge(self):
        """Orders: PK + 1 FK + attributes → node with FK-edge to customer."""
        suggestions = analyze(self._make_ecommerce_tables())
        by_table = {s["table"]: s for s in suggestions}
        o = by_table["orders"]
        assert o["node_role"] == "standard_node"
        assert len(o["edge_roles"]) == 1
        assert o["edge_roles"][0]["fk_column"] == "customer_id"

    def test_order_items_is_edge(self):
        """Order_items: composite PK → standard_edge (bridge table)."""
        suggestions = analyze(self._make_ecommerce_tables())
        by_table = {s["table"]: s for s in suggestions}
        oi = by_table["order_items"]
        assert oi["node_role"] is None
        assert oi["pattern"] == "standard_edge"

    def test_reviews_is_node_with_two_fk_edges(self):
        """Reviews: PK + 2 FKs + rich attributes → node with 2 FK-edges."""
        suggestions = analyze(self._make_ecommerce_tables())
        by_table = {s["table"]: s for s in suggestions}
        r = by_table["reviews"]
        assert r["node_role"] == "standard_node"
        assert len(r["edge_roles"]) == 2
        fk_cols = {e["fk_column"] for e in r["edge_roles"]}
        assert fk_cols == {"product_id", "customer_id"}

    def test_cross_table_fk_resolution(self):
        """FK columns in orders/reviews should resolve to customer/product tables."""
        suggestions = analyze(self._make_ecommerce_tables())
        by_table = {s["table"]: s for s in suggestions}

        order_edge = by_table["orders"]["edge_roles"][0]
        assert order_edge.get("to_table") == "customers"

        review_edges = {e["fk_column"]: e for e in by_table["reviews"]["edge_roles"]}
        assert review_edges["customer_id"].get("to_table") == "customers"
        assert review_edges["product_id"].get("to_table") == "products"


class TestFinancialLedger:
    """Financial double-entry accounting — debit/credit FK columns."""

    def test_journal_entries(self):
        tables = [
            {
                "name": "accounts",
                "columns": [
                    {"name": "account_id", "is_primary_key": True},
                    {"name": "name", "is_primary_key": False},
                    {"name": "account_type", "is_primary_key": False},
                    {"name": "currency", "is_primary_key": False},
                ],
            },
            {
                "name": "journal_entries",
                "columns": [
                    {"name": "entry_id", "is_primary_key": True},
                    {"name": "debit_account_id", "is_primary_key": False},
                    {"name": "credit_account_id", "is_primary_key": False},
                    {"name": "amount", "is_primary_key": False},
                    {"name": "currency", "is_primary_key": False},
                    {"name": "entry_date", "is_primary_key": False},
                    {"name": "description", "is_primary_key": False},
                    {"name": "status", "is_primary_key": False},
                ],
            },
        ]
        suggestions = analyze(tables)
        by_table = {s["table"]: s for s in suggestions}

        je = by_table["journal_entries"]
        assert je["node_role"] == "standard_node"
        assert len(je["edge_roles"]) == 2
        fk_cols = {e["fk_column"] for e in je["edge_roles"]}
        assert fk_cols == {"debit_account_id", "credit_account_id"}

        # Both should resolve to accounts table via stem suffix match
        for edge in je["edge_roles"]:
            assert edge.get("to_table") == "accounts", (
                f"{edge['fk_column']} should resolve to accounts, got {edge.get('to_table')}"
            )


class TestSupplyChain:
    """Supply chain hub table — node with 4+ FK relationships."""

    def test_shipment_hub(self):
        tables = [
            {"name": "suppliers", "columns": [
                {"name": "supplier_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
            ]},
            {"name": "warehouses", "columns": [
                {"name": "warehouse_id", "is_primary_key": True},
                {"name": "location", "is_primary_key": False},
            ]},
            {"name": "carriers", "columns": [
                {"name": "carrier_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
            ]},
            {
                "name": "shipments",
                "columns": [
                    {"name": "shipment_id", "is_primary_key": True},
                    {"name": "supplier_id", "is_primary_key": False},
                    {"name": "warehouse_id", "is_primary_key": False},
                    {"name": "carrier_id", "is_primary_key": False},
                    {"name": "shipped_at", "is_primary_key": False},
                    {"name": "weight_kg", "is_primary_key": False},
                    {"name": "status", "is_primary_key": False},
                    {"name": "tracking_number", "is_primary_key": False},
                ],
            },
        ]
        suggestions = analyze(tables)
        by_table = {s["table"]: s for s in suggestions}

        s = by_table["shipments"]
        assert s["node_role"] == "standard_node"
        assert len(s["edge_roles"]) == 3

        # All FKs should resolve to their target tables
        resolved = {e["fk_column"]: e.get("to_table") for e in s["edge_roles"]}
        assert resolved["supplier_id"] == "suppliers"
        assert resolved["warehouse_id"] == "warehouses"
        assert resolved["carrier_id"] == "carriers"


class TestHealthcare:
    """Healthcare appointment — node with 4 FKs + polymorphic type."""

    def test_appointment_node_with_many_fks(self):
        tables = [
            {"name": "patients", "columns": [
                {"name": "patient_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
            ]},
            {"name": "doctors", "columns": [
                {"name": "doctor_id", "is_primary_key": True},
                {"name": "specialty", "is_primary_key": False},
            ]},
            {"name": "facilities", "columns": [
                {"name": "facility_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
            ]},
            {
                "name": "appointments",
                "columns": [
                    {"name": "appointment_id", "is_primary_key": True},
                    {"name": "patient_id", "is_primary_key": False},
                    {"name": "doctor_id", "is_primary_key": False},
                    {"name": "facility_id", "is_primary_key": False},
                    {"name": "scheduled_at", "is_primary_key": False},
                    {"name": "duration_minutes", "is_primary_key": False},
                    {"name": "appointment_type", "is_primary_key": False},
                    {"name": "status", "is_primary_key": False},
                    {"name": "notes", "is_primary_key": False},
                ],
                "sample": [
                    {"appointment_type": "Routine"},
                    {"appointment_type": "Emergency"},
                    {"appointment_type": "Follow-up"},
                ],
            },
        ]
        suggestions = analyze(tables)
        by_table = {s["table"]: s for s in suggestions}

        appt = by_table["appointments"]
        assert appt["node_role"] == "standard_node"
        assert len(appt["edge_roles"]) == 3  # patient, doctor, facility FKs

        # Also detects polymorphic sub-labels from appointment_type
        assert appt["polymorphic_labels"] is not None
        labels = {pl["label"] for pl in appt["polymorphic_labels"]}
        assert "Routine" in labels
        assert "Emergency" in labels

        # FK resolution
        resolved = {e["fk_column"]: e.get("to_table") for e in appt["edge_roles"]}
        assert resolved["patient_id"] == "patients"
        assert resolved["doctor_id"] == "doctors"
        assert resolved["facility_id"] == "facilities"


class TestRBAC:
    """Role-Based Access Control — junction tables + permission with action column."""

    def test_rbac_schema(self):
        tables = [
            {"name": "users", "columns": [
                {"name": "user_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
                {"name": "email", "is_primary_key": False},
            ]},
            {"name": "roles", "columns": [
                {"name": "role_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
                {"name": "description", "is_primary_key": False},
            ]},
            {"name": "permissions", "columns": [
                {"name": "permission_id", "is_primary_key": True},
                {"name": "resource", "is_primary_key": False},
                {"name": "action", "is_primary_key": False},
                {"name": "scope", "is_primary_key": False},
            ]},
            {"name": "user_roles", "columns": [
                {"name": "user_id", "is_primary_key": True},
                {"name": "role_id", "is_primary_key": True},
                {"name": "granted_at", "is_primary_key": False},
            ]},
            {"name": "role_permissions", "columns": [
                {"name": "role_id", "is_primary_key": True},
                {"name": "permission_id", "is_primary_key": True},
            ]},
        ]
        suggestions = analyze(tables)
        by_table = {s["table"]: s for s in suggestions}

        # Node tables
        assert by_table["users"]["node_role"] == "standard_node"
        assert by_table["roles"]["node_role"] == "standard_node"
        # permissions has "action" column but that shouldn't interfere with PK=1 path
        assert by_table["permissions"]["node_role"] == "standard_node"

        # Junction tables
        assert by_table["user_roles"]["pattern"] == "standard_edge"
        assert by_table["role_permissions"]["pattern"] == "standard_edge"


class TestSelfReferentialClosure:
    """Closure table for hierarchical data (tree/DAG)."""

    def test_category_closure(self):
        tables = [{
            "name": "category_closure",
            "columns": [
                {"name": "ancestor_id", "is_primary_key": True},
                {"name": "descendant_id", "is_primary_key": True},
                {"name": "depth", "is_primary_key": False},
            ],
        }]
        suggestions = analyze(tables)
        s = suggestions[0]
        assert s["pattern"] == "standard_edge"
        assert s["node_role"] is None


class TestDenormalizedSourceTarget:
    """Tests source_/target_ prefix detection for denormalized edges."""

    def test_lineage_source_target(self):
        """ETL lineage with source_/target_ naming."""
        tables = [{
            "name": "data_lineage",
            "columns": [
                {"name": "lineage_id", "is_primary_key": True},
                {"name": "source_table", "is_primary_key": False},
                {"name": "source_schema", "is_primary_key": False},
                {"name": "target_table", "is_primary_key": False},
                {"name": "target_schema", "is_primary_key": False},
                {"name": "transform_type", "is_primary_key": False},
            ],
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "denormalized_edge"

    def test_messaging_from_to(self):
        """Email/messaging with from_/to_ naming."""
        tables = [{
            "name": "messages",
            "columns": [
                {"name": "message_id", "is_primary_key": True},
                {"name": "from_address", "is_primary_key": False},
                {"name": "to_address", "is_primary_key": False},
                {"name": "subject", "is_primary_key": False},
                {"name": "body", "is_primary_key": False},
                {"name": "sent_at", "is_primary_key": False},
            ],
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "denormalized_edge"


class TestIoTTelemetry:
    """IoT sensor data — append-only event tables."""

    def test_sensor_readings_no_pk(self):
        """No PK, no conventional FK naming → flat_table."""
        tables = [{
            "name": "sensor_readings",
            "columns": [
                {"name": "event_time", "is_primary_key": False, "type": "DateTime"},
                {"name": "device_serial", "is_primary_key": False},
                {"name": "sensor_type", "is_primary_key": False},
                {"name": "value", "is_primary_key": False},
                {"name": "unit", "is_primary_key": False},
            ],
        }]
        suggestions = analyze(tables)
        # No FK-like naming, no PK → flat_table (correctly flagged for review)
        assert suggestions[0]["pattern"] == "flat_table"

    def test_device_events_with_fk(self):
        """Event table with device_id FK → fk_edge."""
        tables = [{
            "name": "device_events",
            "columns": [
                {"name": "event_time", "is_primary_key": False, "type": "DateTime"},
                {"name": "device_id", "is_primary_key": False},
                {"name": "event_type", "is_primary_key": False},
                {"name": "payload", "is_primary_key": False},
            ],
        }]
        suggestions = analyze(tables)
        # Single FK + no PK → fk_edge
        assert suggestions[0]["pattern"] == "fk_edge"


class TestKnowledgeGraph:
    """Knowledge graph triple store pattern."""

    def test_triples_table(self):
        tables = [{
            "name": "triples",
            "columns": [
                {"name": "subject_id", "is_primary_key": True},
                {"name": "predicate", "is_primary_key": True},
                {"name": "object_id", "is_primary_key": True},
                {"name": "confidence", "is_primary_key": False},
                {"name": "source", "is_primary_key": False},
            ],
        }]
        suggestions = analyze(tables)
        # Composite PK → standard_edge
        assert suggestions[0]["pattern"] == "standard_edge"

    def test_entity_attribute_value(self):
        """EAV table — composite PK, attribute storage."""
        tables = [{
            "name": "entity_attributes",
            "columns": [
                {"name": "entity_id", "is_primary_key": True},
                {"name": "attribute_name", "is_primary_key": True},
                {"name": "value", "is_primary_key": False},
                {"name": "data_type", "is_primary_key": False},
            ],
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_edge"
