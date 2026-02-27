"""Tests for schema analyzer."""

import pytest
from cg_schema.analyzer import analyze, determine_pattern, generate_reason


class TestPatternDetection:
    """Test schema pattern detection."""

    def test_standard_node_single_pk(self):
        """Standard node with single primary key."""
        result = determine_pattern(["user_id"], [], [])
        assert result == "standard_node"

    def test_standard_edge_multiple_fk(self):
        """Standard edge with multiple foreign keys."""
        result = determine_pattern([], ["user_id", "post_id"], [])
        assert result == "standard_edge"

    def test_fk_node_single_pk_single_fk(self):
        """FK node with single PK and single FK - node with reference to another node."""
        result = determine_pattern(["post_id"], ["user_id"], [])
        assert result == "fk_node"

    def test_denormalized_edge_origin_dest(self):
        """Denormalized edge with origin/dest columns."""
        columns = [
            {"name": "origin_user_id"},
            {"name": "dest_user_id"},
        ]
        result = determine_pattern([], [], columns)
        assert result == "denormalized_edge"

    def test_polymorphic_edge_type_column(self):
        """Polymorphic edge with type column."""
        columns = [{"name": "rel_type"}]
        result = determine_pattern([], ["from_id", "to_id"], columns)
        assert result == "polymorphic_edge"

    def test_composite_pk_junction_table(self):
        """Junction table with composite PK (only PK columns)."""
        columns = [
            {"name": "user_id_1"},
            {"name": "user_id_2"},
            {"name": "since"},
        ]
        result = determine_pattern(["user_id_1", "user_id_2"], [], columns)
        assert result == "standard_edge"

    def test_flat_table_no_keys(self):
        """Flat table with no PK/FK."""
        result = determine_pattern([], [], [])
        assert result == "flat_table"

    def test_denormalized_edge_single_pk_with_origin_dest(self):
        """Denormalized edge with single PK and origin/dest properties (flights_denorm)."""
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
        """Standard edge junction table with timestamp (flights)."""
        columns = [
            {"name": "origin_airport"},
            {"name": "dest_airport"},
            {"name": "flight_date"},
        ]
        result = determine_pattern(["origin_airport", "dest_airport", "flight_date"], [], columns)
        assert result == "standard_edge"

    def test_polymorphic_edge_interactions(self):
        """Polymorphic edge with type columns (interactions table)."""
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


class TestReasonGeneration:
    """Test human-readable reason generation."""

    def test_standard_node_reason(self):
        """Generate reason for standard node."""
        reason = generate_reason("node", "standard_node", ["user_id"], [])
        assert "primary key" in reason.lower() or "node" in reason.lower()

    def test_standard_edge_reason(self):
        """Generate reason for standard edge."""
        reason = generate_reason("edge", "standard_edge", [], ["user_id", "post_id"])
        assert "foreign key" in reason.lower() or "edge" in reason.lower()


class TestRealWorldSchemas:
    """Test against real-world schema patterns from ClickHouse example datasets."""

    def test_social_users(self):
        """Social users - standard node."""
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

    def test_social_follows(self):
        """Social follows - standard edge junction table."""
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

    def test_ldbc_person(self):
        """LDBC person - standard node."""
        tables = [{
            "name": "person",
            "columns": [
                {"name": "personId", "is_primary_key": True},
                {"name": "firstName", "is_primary_key": False},
                {"name": "lastName", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_node"

    def test_ldbc_person_knows_person(self):
        """LDBC person_knows_person - standard edge junction table."""
        tables = [{
            "name": "person_knows_person",
            "columns": [
                {"name": "person1Id", "is_primary_key": True},
                {"name": "person2Id", "is_primary_key": True},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "standard_edge"

    def test_ldbc_comment(self):
        """LDBC comment - fk_edge (PK + 2 FKs)."""
        tables = [{
            "name": "comment",
            "columns": [
                {"name": "commentId", "is_primary_key": True},
                {"name": "content", "is_primary_key": False},
                {"name": "creatorId", "is_primary_key": False},
                {"name": "replyOfCommentId", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert suggestions[0]["pattern"] == "fk_edge"

    def test_travel_flights(self):
        """Travel flights - standard edge with timestamp."""
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
        """Travel flights_denorm - denormalized edge."""
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
        """Brahmand interactions - polymorphic edge."""
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
        """Brahmand groups - standard node."""
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
        """Brahmand memberships - standard edge."""
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
        """Community interactions - polymorphic edge."""
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
        """Data security users - standard node."""
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
        """Data security permissions - standard edge."""
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
        """Filesystem files - node with FK reference."""
        tables = [{
            "name": "files",
            "columns": [
                {"name": "file_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
                {"name": "folder_id", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        # Single PK + 1 FK = fk_node (file belongs to folder)
        assert suggestions[0]["pattern"] == "fk_node"

    def test_filesystem_folders(self):
        """Filesystem folders - standard node with self-reference."""
        tables = [{
            "name": "folders",
            "columns": [
                {"name": "folder_id", "is_primary_key": True},
                {"name": "name", "is_primary_key": False},
                {"name": "parent_folder_id", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        # Single PK + 1 FK = fk_node (folder has parent reference)
        assert suggestions[0]["pattern"] == "fk_node"

    def test_lineage_file_lineage(self):
        """Lineage file_lineage - standard edge."""
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
        """Security conn - flat table / event log."""
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
        # Composite PK with no additional properties = standard_edge (junction)
        assert suggestions[0]["pattern"] == "standard_edge"

    def test_db_multi_tenant_posts(self):
        """Multi-tenant posts - node with FK reference."""
        tables = [{
            "name": "posts",
            "columns": [
                {"name": "post_id", "is_primary_key": True},
                {"name": "user_id", "is_primary_key": False},
                {"name": "content", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        # Single PK + 1 FK = fk_node (post belongs to user)
        assert suggestions[0]["pattern"] == "fk_node"

    def test_db_multi_tenant_user_follows(self):
        """Multi-tenant user_follows - standard edge."""
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
        """Polymorphic interactions - polymorphic edge."""
        tables = [{
            "name": "interactions",
            "columns": [
                {"name": "from_id", "is_primary_key": False},
                {"name": "to_id", "is_primary_key": False},
                {"name": "type", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        # Has type column but only 2 FKs, should be polymorphic
        assert suggestions[0]["pattern"] == "polymorphic_edge"


class TestColumnNamingPatterns:
    """Test column naming patterns for FK/PK detection."""

    def test_camel_case_pk(self):
        """Test camelCase primary keys (personId, userId)."""
        tables = [{
            "name": "test",
            "columns": [
                {"name": "userId", "is_primary_key": True},
            ]
        }]
        suggestions = analyze(tables)
        assert "userId" in suggestions[0]["pk_columns"]

    def test_camel_case_fk(self):
        """Test camelCase foreign keys."""
        tables = [{
            "name": "test",
            "columns": [
                {"name": "creatorId", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert "creatorId" in suggestions[0]["fk_columns"]

    def test_snake_case_fk(self):
        """Test snake_case foreign keys."""
        tables = [{
            "name": "test",
            "columns": [
                {"name": "user_id", "is_primary_key": False},
            ]
        }]
        suggestions = analyze(tables)
        assert "user_id" in suggestions[0]["fk_columns"]

    def test_uppercase_id(self):
        """Test uppercase ID columns."""
        tables = [{
            "name": "test",
            "columns": [
                {"name": "ID", "is_primary_key": True},
            ]
        }]
        suggestions = analyze(tables)
        assert "ID" in suggestions[0]["pk_columns"]

    def test_composite_key_naming(self):
        """Test various composite key patterns."""
        # Test userId + postId (camelCase)
        result = determine_pattern(["userId", "postId"], [], [])
        assert result == "standard_edge"
        
        # Test user_id + post_id (snake_case)
        result = determine_pattern(["user_id", "post_id"], [], [])
        assert result == "standard_edge"

    def test_prefixed_id_columns(self):
        """Test prefixed ID columns (e.g., id_user, id_post)."""
        result = determine_pattern(["id_user", "id_post"], [], [])
        # Should detect these as PKs for a junction table
        assert result == "standard_edge"


class TestValueAnalysis:
    """Test value-based analysis from sample data."""

    def test_email_detection(self):
        """Test email detection in sample values."""
        from cg_schema.analyzer import analyze_sample_values
        
        sample = [
            {"email": "user@example.com", "name": "John"},
            {"email": "jane@example.com", "name": "Jane"},
        ]
        
        result = analyze_sample_values(sample)
        assert result["enabled"] is True
        assert len(result["patterns"]) > 0
        assert any(p["type"] == "email" for p in result["patterns"])

    def test_url_detection(self):
        """Test URL detection in sample values."""
        from cg_schema.analyzer import analyze_sample_values
        
        sample = [
            {"url": "https://example.com/page1", "name": "Page 1"},
        ]
        
        result = analyze_sample_values(sample)
        assert result["enabled"] is True
        assert any(p["type"] == "url" for p in result["patterns"])

    def test_uuid_detection(self):
        """Test UUID detection in sample values."""
        from cg_schema.analyzer import analyze_sample_values
        
        sample = [
            {"id": "550e8400-e29b-41d4-a716-446655440000", "name": "Test"},
        ]
        
        result = analyze_sample_values(sample)
        assert result["enabled"] is True
        assert any(p["type"] == "uuid" for p in result["patterns"])

    def test_empty_sample(self):
        """Test with empty sample data."""
        from cg_schema.analyzer import analyze_sample_values
        
        result = analyze_sample_values([])
        assert result["enabled"] is False
