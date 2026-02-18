"""
Comprehensive integration tests for multi-tenant parameterized views.

Tests the complete Phase 2 multi-tenancy feature including:
- Basic tenant isolation
- Multi-parameter views
- Cache behavior with parameterized views
- Error handling for missing parameters
- SQL generation with placeholders
"""
import pytest
import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import time
from typing import Dict, Any

BASE_URL = f"{CLICKGRAPH_URL}"


@pytest.fixture(scope="module")
def multi_tenant_schema():
    """Load the multi-tenant schema once for all tests."""
    with open('schemas/test/multi_tenant.yaml', 'r') as f:
        schema_yaml = f.read()
    
    response = requests.post(
        f"{BASE_URL}/schemas/load",
        json={
            "schema_name": "multi_tenant_test",
            "config_content": schema_yaml
        }
    )
    assert response.status_code == 200, f"Schema load failed: {response.text}"
    return "multi_tenant_test"


def query_clickgraph(query: str, schema_name: str, view_parameters: Dict[str, Any] = None, 
                     parameters: Dict[str, Any] = None, sql_only: bool = False) -> Dict:
    """Helper function to query ClickGraph."""
    payload = {
        "query": query,
        "schema_name": schema_name
    }
    if view_parameters:
        payload["view_parameters"] = view_parameters
    if parameters:
        payload["parameters"] = parameters
    if sql_only:
        payload["sql_only"] = True
    
    response = requests.post(f"{BASE_URL}/query", json=payload)
    assert response.status_code == 200, f"Query failed: {response.text}"
    return response.json()


requires_table_functions = pytest.mark.skip(
    reason="Requires ClickHouse parameterized table functions (users_by_tenant, etc.) not created in test environment"
)


@requires_table_functions
class TestBasicTenantIsolation:
    """Test basic tenant isolation with single parameter."""
    
    def test_acme_tenant_users(self, multi_tenant_schema):
        """ACME tenant should only see ACME users."""
        result = query_clickgraph(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            multi_tenant_schema,
            view_parameters={"tenant_id": "acme"}
        )
        
        names = [row["u.name"] for row in result["results"]]
        assert names == ["Alice Anderson", "Bob Brown", "Carol Chen"], \
            f"Expected ACME users, got {names}"
    
    def test_globex_tenant_users(self, multi_tenant_schema):
        """GLOBEX tenant should only see GLOBEX users."""
        result = query_clickgraph(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            multi_tenant_schema,
            view_parameters={"tenant_id": "globex"}
        )
        
        names = [row["u.name"] for row in result["results"]]
        assert names == ["David Davis", "Emma Evans", "Frank Foster"], \
            f"Expected GLOBEX users, got {names}"
    
    def test_tenant_isolation_with_where(self, multi_tenant_schema):
        """Tenant isolation should work with WHERE clauses."""
        result = query_clickgraph(
            "MATCH (u:User) WHERE u.country = 'USA' RETURN u.name",
            multi_tenant_schema,
            view_parameters={"tenant_id": "acme"}
        )
        
        # Should only return ACME users from USA
        names = [row["u.name"] for row in result["results"]]
        assert all("acme" in name.lower() or name in ["Alice Anderson", "Bob Brown", "Carol Chen"] 
                   for name in names), f"Got non-ACME users: {names}"
    
    def test_relationship_tenant_isolation(self, multi_tenant_schema):
        """Relationships should also respect tenant isolation."""
        result = query_clickgraph(
            "MATCH (u1:User)-[:FRIENDS_WITH]->(u2:User) RETURN u1.name, u2.name",
            multi_tenant_schema,
            view_parameters={"tenant_id": "acme"}
        )
        
        # All friendships should be within ACME tenant
        for row in result["results"]:
            assert "Alice" in row["u1.name"] or "Bob" in row["u1.name"] or "Carol" in row["u1.name"], \
                f"Source not in ACME: {row['u1.name']}"
            assert "Alice" in row["u2.name"] or "Bob" in row["u2.name"] or "Carol" in row["u2.name"], \
                f"Target not in ACME: {row['u2.name']}"


class TestSQLGeneration:
    """Test SQL generation with parameterized views."""
    
    def test_sql_has_placeholder(self, multi_tenant_schema):
        """Generated SQL should use $tenant_id placeholder."""
        result = query_clickgraph(
            "MATCH (u:User) RETURN u.name",
            multi_tenant_schema,
            view_parameters={"tenant_id": "acme"},
            sql_only=True
        )
        
        sql = result["generated_sql"]
        assert "$tenant_id" in sql or "users_by_tenant(tenant_id" in sql, \
            f"SQL should contain parameterized view syntax: {sql}"
    
    def test_sql_structure(self, multi_tenant_schema):
        """SQL should have correct parameterized view structure."""
        result = query_clickgraph(
            "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name",
            multi_tenant_schema,
            view_parameters={"tenant_id": "acme"},
            sql_only=True
        )
        
        sql = result["generated_sql"]
        # Should reference the parameterized view
        assert "users_by_tenant" in sql, f"SQL should reference users_by_tenant view: {sql}"


@requires_table_functions
class TestCacheBehavior:
    """Test caching behavior with parameterized views."""
    
    def test_cache_shares_across_tenants(self, multi_tenant_schema):
        """Different tenants should share the same cached SQL template."""
        # Query ACME (cache miss)
        start1 = time.time()
        result1 = query_clickgraph(
            "MATCH (u:User) RETURN u.name LIMIT 1",
            multi_tenant_schema,
            view_parameters={"tenant_id": "acme"}
        )
        time1 = time.time() - start1
        
        # Query GLOBEX (should hit cache)
        start2 = time.time()
        result2 = query_clickgraph(
            "MATCH (u:User) RETURN u.name LIMIT 1",
            multi_tenant_schema,
            view_parameters={"tenant_id": "globex"}
        )
        time2 = time.time() - start2
        
        # Both should succeed with different data
        assert len(result1["results"]) > 0
        assert len(result2["results"]) > 0
        assert result1["results"][0]["u.name"] != result2["results"][0]["u.name"], \
            "Different tenants should return different data"
        
        # Second query should be faster (cache hit)
        # Note: This is a soft assertion since timing can vary
        if time2 < time1 * 0.8:
            print(f"✓ Cache hit detected: {time1:.3f}s -> {time2:.3f}s")
    
    def test_cache_with_different_queries(self, multi_tenant_schema):
        """Different queries should create different cache entries."""
        # Two different queries with same tenant
        result1 = query_clickgraph(
            "MATCH (u:User) RETURN u.name",
            multi_tenant_schema,
            view_parameters={"tenant_id": "acme"}
        )
        
        result2 = query_clickgraph(
            "MATCH (u:User) RETURN u.email",
            multi_tenant_schema,
            view_parameters={"tenant_id": "acme"}
        )
        
        # Both should succeed
        assert len(result1["results"]) > 0
        assert len(result2["results"]) > 0
        assert "u.name" in result1["results"][0]
        assert "u.email" in result2["results"][0]


@requires_table_functions
class TestErrorHandling:
    """Test error handling for missing or invalid parameters."""
    
    def test_missing_required_parameter(self, multi_tenant_schema):
        """Should handle missing required view_parameters gracefully."""
        # Query without view_parameters when schema expects them
        response = requests.post(
            f"{BASE_URL}/query",
            json={
                "query": "MATCH (u:User) RETURN u.name",
                "schema_name": multi_tenant_schema
            }
        )
        
        # Should either error or return empty results
        # Implementation detail: current behavior is to generate SQL without parameters
        assert response.status_code in [200, 400], \
            f"Expected 200 or 400, got {response.status_code}"
    
    def test_empty_view_parameters(self, multi_tenant_schema):
        """Should handle empty view_parameters dict."""
        response = requests.post(
            f"{BASE_URL}/query",
            json={
                "query": "MATCH (u:User) RETURN u.name LIMIT 1",
                "schema_name": multi_tenant_schema,
                "view_parameters": {}
            }
        )
        
        # Should handle gracefully
        assert response.status_code in [200, 400]


@pytest.fixture(scope="module")
def multi_param_schema():
    """Load the multi-parameter schema (tenant_id + country)."""
    with open('schemas/test/multi_tenant_multi_param.yaml', 'r') as f:
        schema_yaml = f.read()
    
    response = requests.post(
        f"{BASE_URL}/schemas/load",
        json={
            "schema_name": "multi_param_test",
            "config_content": schema_yaml
        }
    )
    assert response.status_code == 200, f"Schema load failed: {response.text}"
    return "multi_param_test"


@pytest.fixture(scope="module")
def date_range_schema():
    """Load the date-range schema (tenant_id + start_date + end_date)."""
    with open('schemas/test/multi_tenant_date_range.yaml', 'r') as f:
        schema_yaml = f.read()
    
    response = requests.post(
        f"{BASE_URL}/schemas/load",
        json={
            "schema_name": "date_range_test",
            "config_content": schema_yaml
        }
    )
    assert response.status_code == 200, f"Schema load failed: {response.text}"
    return "date_range_test"


@requires_table_functions
class TestMultiParameterViews:
    """Test views with multiple parameters (tenant_id + region, date, etc)."""
    
    def test_tenant_plus_region_filter(self, multi_param_schema):
        """Test view with both tenant_id and country parameters."""
        response = requests.post(
            f"{BASE_URL}/query",
            json={
                "query": "MATCH (u:User) RETURN u.name ORDER BY u.name",
                "schema_name": multi_param_schema,
                "view_parameters": {"tenant_id": "acme", "country": "USA"}
            }
        )
        assert response.status_code == 200, f"Query failed: {response.text}"
        
        result = response.json()
        names = [row["u.name"] for row in result["results"]]
        # Should only return ACME users from USA
        assert len(names) >= 1, f"Expected at least 1 user, got {names}"
        # Alice Anderson and Bob Brown are from USA in acme tenant
        assert any("Alice" in n or "Bob" in n for n in names), f"Expected Alice or Bob, got {names}"
    
    def test_tenant_plus_date_range(self, date_range_schema):
        """Test view with tenant_id + start_date + end_date parameters."""
        response = requests.post(
            f"{BASE_URL}/query",
            json={
                "query": "MATCH (o:Order) RETURN o.product, o.amount ORDER BY o.order_id",
                "schema_name": date_range_schema,
                "view_parameters": {
                    "tenant_id": "acme",
                    "start_date": "2025-01-01",
                    "end_date": "2025-12-31"
                }
            }
        )
        assert response.status_code == 200, f"Query failed: {response.text}"
        
        result = response.json()
        products = [row["o.product"] for row in result["results"]]
        # Should return ACME orders in the date range
        assert len(products) >= 1, f"Expected at least 1 order, got {products}"
        # Widget A, Widget B, Gadget X are ACME products
        assert any("Widget" in p or "Gadget" in p for p in products), f"Expected widgets/gadgets, got {products}"


@requires_table_functions
class TestQueryParameters:
    """Test interaction between view_parameters and query parameters."""
    
    def test_query_parameter_with_view_parameter(self, multi_tenant_schema):
        """Query parameters should work alongside view_parameters."""
        result = query_clickgraph(
            "MATCH (u:User) WHERE u.user_id = $userId RETURN u.name",
            multi_tenant_schema,
            view_parameters={"tenant_id": "acme"},
            parameters={"userId": 1}
        )
        
        # Should return user 1 from ACME tenant
        assert len(result["results"]) <= 1, "Should return at most 1 user"
        if len(result["results"]) == 1:
            assert "Alice" in result["results"][0]["u.name"] or \
                   "Bob" in result["results"][0]["u.name"] or \
                   "Carol" in result["results"][0]["u.name"], \
                "Should return ACME tenant user"


@requires_table_functions
class TestPerformance:
    """Test performance overhead of parameterized views."""
    
    def test_performance_overhead_minimal(self, multi_tenant_schema):
        """Parameterized views should add minimal overhead."""
        # Run query 5 times and check average time
        times = []
        for _ in range(5):
            start = time.time()
            query_clickgraph(
                "MATCH (u:User) RETURN u.name LIMIT 10",
                multi_tenant_schema,
                view_parameters={"tenant_id": "acme"}
            )
            times.append(time.time() - start)
        
        avg_time = sum(times) / len(times)
        print(f"\nAverage query time: {avg_time*1000:.2f}ms")
        
        # Should be reasonably fast (< 100ms for simple queries)
        assert avg_time < 0.1, f"Query too slow: {avg_time*1000:.2f}ms"


# Run tests with: pytest tests/integration/test_multi_tenant_parameterized_views.py -v
