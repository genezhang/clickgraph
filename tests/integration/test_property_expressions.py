"""
Integration tests for property mapping expressions in schema configuration.

Tests that YAML property_mappings support ClickHouse expressions:
- String functions (concat, splitByChar)
- Date functions (dateDiff, toDate)
- Type conversions (toUInt8, toFloat64)
- Conditional logic (CASE WHEN, multiIf)
- JSON extraction (JSONExtractString)
- Mathematical operations
- Boolean conversions
"""

import pytest
import requests
import os

BASE_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")

# Schema for property expression tests
PROPERTY_EXPR_SCHEMA = "property_expressions"


def query(cypher: str, schema_name: str = PROPERTY_EXPR_SCHEMA):
    """Execute a Cypher query."""
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher, "schema_name": schema_name}
    )
    assert response.status_code == 200, f"Query failed: {response.text}"
    return response.json()


class TestStringExpressions:
    """Test string concatenation and manipulation expressions."""
    
    def test_concat_full_name(self):
        """Test concat() for full name construction."""
        result = query("""
            MATCH (u:User) 
            WHERE u.user_id = 1
            RETURN u.full_name
        """)
        assert len(result["results"]) == 1
        assert result["results"][0]["u.full_name"] == "Alice Smith"
    
    def test_concat_with_city(self):
        """Test concat() with multiple fields including literals."""
        result = query("""
            MATCH (u:User) 
            WHERE u.user_id = 2
            RETURN u.display_name
        """)
        assert len(result["results"]) == 1
        assert result["results"][0]["u.display_name"] == "Bob (London)"
    
    def test_split_by_char_array(self):
        """Test splitByChar() creating arrays from CSV strings."""
        result = query("""
            MATCH (u:User) 
            WHERE u.user_id = 6
            RETURN u.tags_array, u.tag_count
        """)
        assert len(result["results"]) == 1
        assert result["results"][0]["u.tag_count"] == 3  # 'gaming,music,sports'


class TestDateExpressions:
    """Test date calculation and conversion expressions."""
    
    def test_date_diff_calculation(self):
        """Test dateDiff() for calculating days since registration."""
        result = query("""
            MATCH (u:User) 
            WHERE u.user_id = 1
            RETURN u.account_age_days
        """)
        assert len(result["results"]) == 1
        # User registered 2020-01-15, should be ~5 years = ~1825+ days
        assert result["results"][0]["u.account_age_days"] > 1800
    
    @pytest.mark.xfail(reason="Data-dependent: registration date offsets shift over time, count may vary")
    def test_date_comparison_recent_user(self):
        """Test date comparison for recent users (within 30 days)."""
        result = query("""
            MATCH (u:User) 
            WHERE dateDiff('day', u.registration_date, today()) <= 30
            RETURN u.user_id, u.full_name
            ORDER BY u.user_id
        """)
        # User 10 registered within 30 days (28 days ago)
        assert len(result["results"]) == 1
        assert result["results"][0]["u.user_id"] == 10
    
    def test_to_date_conversion(self):
        """Test toDate() converting string to Date type."""
        result = query("""
            MATCH (u:User) 
            WHERE u.user_id = 1
            RETURN u.birth_date
        """)
        assert len(result["results"]) == 1
        # birth_date_str was '1990-05-20'
        assert "1990-05-20" in str(result["results"][0]["u.birth_date"])


class TestTypeConversions:
    """Test type conversion expressions."""
    
    def test_to_uint8_conversion(self):
        """Test toUInt8() converting string to integer."""
        result = query("""
            MATCH (u:User) 
            WHERE u.user_id = 1
            RETURN u.age_int
        """)
        assert len(result["results"]) == 1
        assert result["results"][0]["u.age_int"] == 34
    
    def test_to_float64_conversion(self):
        """Test toFloat64() converting string to float."""
        result = query("""
            MATCH (u:User) 
            WHERE u.user_id = 1
            RETURN u.score_float
        """)
        assert len(result["results"]) == 1
        assert abs(result["results"][0]["u.score_float"] - 1250.5) < 0.01


class TestConditionalExpressionsCaseWhen:
    """Test CASE WHEN conditional expressions."""
    
    def test_case_when_tier_gold(self):
        """Test CASE WHEN for gold tier (score >= 1000)."""
        result = query("""
            MATCH (u:User) 
            WHERE u.score >= 1000
            RETURN u.user_id, u.full_name, u.tier
            ORDER BY u.user_id
        """)
        assert len(result["results"]) == 2  # Users 1 and 2
        assert all(r["u.tier"] == "gold" for r in result["results"])
    
    def test_case_when_tier_silver(self):
        """Test CASE WHEN for silver tier (500 <= score < 1000)."""
        result = query("""
            MATCH (u:User) 
            WHERE u.score >= 500 AND u.score < 1000
            RETURN u.user_id, u.tier
            ORDER BY u.user_id
        """)
        assert len(result["results"]) == 5  # Users 3, 4, 7, 9, 12
        assert all(r["u.tier"] == "silver" for r in result["results"])
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_case_when_tier_bronze(self):
        """Test CASE WHEN for bronze tier (score < 500)."""
        result = query("""
            MATCH (u:User) 
            WHERE u.tier = 'bronze'
            RETURN count(*) as bronze_count
        """)
        assert result["results"][0]["bronze_count"] >= 4
    
    def test_case_when_age_groups(self):
        """Test CASE WHEN for age group classification."""
        result = query("""
            MATCH (u:User)
            WHERE u.user_id IN [11, 3, 12]
            RETURN u.user_id, u.age_group
            ORDER BY u.user_id
        """)
        assert len(result["results"]) == 3
        assert result["results"][0]["u.age_group"] == "adult"   # User 3, age 29
        assert result["results"][1]["u.age_group"] == "minor"   # User 11, age 14
        assert result["results"][2]["u.age_group"] == "senior"  # User 12, age 66


class TestConditionalExpressionsMultiIf:
    """Test multiIf() conditional expressions (ClickHouse optimized)."""
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_multi_if_status_deleted(self):
        """Test multiIf() for deleted user status."""
        result = query("""
            MATCH (u:User) 
            WHERE u.status = 'deleted'
            RETURN u.user_id, u.full_name, u.status
        """)
        assert len(result["results"]) == 1
        assert result["results"][0]["u.user_id"] == 7
        assert result["results"][0]["u.status"] == "deleted"
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_multi_if_status_banned(self):
        """Test multiIf() for banned user status."""
        result = query("""
            MATCH (u:User) 
            WHERE u.status = 'banned'
            RETURN u.user_id, u.status
        """)
        assert len(result["results"]) == 1
        assert result["results"][0]["u.user_id"] == 8
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_multi_if_status_active(self):
        """Test multiIf() for active user status."""
        result = query("""
            MATCH (u:User) 
            WHERE u.status = 'active'
            RETURN count(*) as active_count
        """)
        # Most users should be active
        assert result["results"][0]["active_count"] >= 8
    
    def test_multi_if_priority_tiers(self):
        """Test multiIf() for priority classification."""
        result = query("""
            MATCH (u:User)
            WHERE u.user_id IN [1, 3, 5]
            RETURN u.user_id, u.priority
            ORDER BY u.user_id
        """)
        assert len(result["results"]) == 3
        # All three users are active (is_active = 1), so priority is 'high' per multiIf logic
        assert result["results"][0]["u.priority"] == "high"    # User 1
        assert result["results"][1]["u.priority"] == "high"    # User 3
        assert result["results"][2]["u.priority"] == "high"    # User 5


class TestJSONExpressions:
    """Test JSON extraction expressions."""
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_json_extract_string(self):
        """Test JSONExtractString() for extracting JSON fields."""
        result = query("""
            MATCH (u:User) 
            WHERE u.metadata_key = 'premium'
            RETURN u.user_id, u.full_name, u.metadata_key
            ORDER BY u.user_id
        """)
        # Users 1 and 2 have subscription_type: premium
        assert len(result["results"]) == 2
        assert result["results"][0]["u.user_id"] == 1
        assert result["results"][1]["u.user_id"] == 2


class TestMathematicalExpressions:
    """Test mathematical operation expressions."""
    
    def test_division_normalization(self):
        """Test division for score normalization."""
        result = query("""
            MATCH (u:User) 
            WHERE u.user_id = 2
            RETURN u.score_normalized
        """)
        assert len(result["results"]) == 1
        # User 2 has score 1500, normalized should be 1.5
        assert abs(result["results"][0]["u.score_normalized"] - 1.5) < 0.01
    
    def test_addition_bonus(self):
        """Test addition for bonus score calculation."""
        result = query("""
            MATCH (u:User) 
            WHERE u.user_id = 5
            RETURN u.bonus_score
        """)
        assert len(result["results"]) == 1
        # User 5 has score 250, bonus should be 350
        assert result["results"][0]["u.bonus_score"] == 350


class TestBooleanExpressions:
    """Test boolean conversion expressions."""
    
    def test_boolean_premium_conversion(self):
        """Test boolean conversion from integer."""
        result = query("""
            MATCH (u:User) 
            WHERE u.is_premium_bool = true
            RETURN count(*) as premium_count
        """)
        # Users 1, 2, 12 have is_premium = 1
        assert result["results"][0]["premium_count"] == 3
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_boolean_has_metadata(self):
        """Test boolean check for non-empty string."""
        result = query("""
            MATCH (u:User) 
            WHERE u.has_metadata = true
            RETURN count(*) as with_metadata
        """)
        # Users with non-empty metadata_json
        assert result["results"][0]["with_metadata"] >= 8


class TestEdgePropertyExpressions:
    """Test property expressions on relationship/edge definitions."""
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_edge_date_diff(self):
        """Test dateDiff() on edge properties."""
        result = query("""
            MATCH (u1:User)-[f:FOLLOWS]->(u2:User)
            WHERE f.follow_age_days < 10
            RETURN u1.user_id, u2.user_id, f.follow_age_days
            ORDER BY f.follow_age_days
        """)
        # Should return recent follows (within 7 days)
        assert len(result["results"]) >= 3
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_edge_is_recent_follow(self):
        """Test boolean expression on edge for recent follows."""
        result = query("""
            MATCH (u1:User)-[f:FOLLOWS]->(u2:User)
            WHERE f.is_recent_follow = true
            RETURN count(*) as recent_follows
        """)
        # Follows 1, 2, 3, 9, 10 are recent (within 7 days)
        assert result["results"][0]["recent_follows"] >= 3
    
    def test_edge_relationship_strength(self):
        """Test mathematical expression on edge properties."""
        result = query("""
            MATCH (u1:User)-[f:FOLLOWS]->(u2:User)
            WHERE u1.user_id = 1 AND u2.user_id = 2
            RETURN f.relationship_strength
        """)
        assert len(result["results"]) == 1
        # interaction_count = 150, strength = 1.5
        assert abs(result["results"][0]["f.relationship_strength"] - 1.5) < 0.01
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_edge_strength_tier_case_when(self):
        """Test CASE WHEN on edge for relationship strength tiers."""
        result = query("""
            MATCH (u1:User)-[f:FOLLOWS]->(u2:User)
            WHERE f.strength_tier = 'strong'
            RETURN count(*) as strong_relationships
        """)
        # Follows with interaction_count >= 100
        assert result["results"][0]["strong_relationships"] >= 3


class TestComplexExpressionQueries:
    """Test complex queries combining multiple expression types."""
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_combined_filters(self):
        """Test query with multiple expression-based filters."""
        result = query("""
            MATCH (u:User)
            WHERE u.tier = 'gold' 
              AND u.status = 'active'
              AND u.is_premium_bool = true
            RETURN u.user_id, u.full_name, u.tier, u.status
            ORDER BY u.user_id
        """)
        # Users 1 and 2 meet all criteria
        assert len(result["results"]) == 2
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_aggregation_with_expressions(self):
        """Test aggregations on expression-computed properties."""
        result = query("""
            MATCH (u:User)
            RETURN u.tier, count(*) as count, avg(u.score_normalized) as avg_norm_score
            ORDER BY u.tier
        """)
        assert len(result["results"]) == 3  # bronze, gold, silver
        tiers = [r["u.tier"] for r in result["results"]]
        assert "gold" in tiers and "silver" in tiers and "bronze" in tiers
    
    @pytest.mark.xfail(reason="Code bug: expression-based property mappings get backtick-quoted in WHERE/GROUP BY instead of expanded")
    def test_path_with_edge_expressions(self):
        """Test path traversal with edge expression filters."""
        result = query("""
            MATCH (u1:User)-[f:FOLLOWS]->(u2:User)
            WHERE u1.tier = 'gold' 
              AND f.is_recent_follow = true
              AND f.strength_tier IN ['strong', 'moderate']
            RETURN u1.full_name, u2.full_name, f.strength_tier
        """)
        # Should find recent strong/moderate follows from gold users
        assert len(result["results"]) >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

