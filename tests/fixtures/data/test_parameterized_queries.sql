-- Create actual ClickHouse table functions for parameterized views
-- Note: ClickHouse doesn't have CREATE FUNCTION for table functions like PostgreSQL
-- Instead, we'll use a different approach: parameterized views via WHERE clause pushdown

-- Alternative approach: Use views that can be filtered
-- The key insight: Our SQL generator should add WHERE clauses, not use table() syntax

-- For testing purposes, let's verify the data is accessible:
SELECT 'Test 1: Direct query with WHERE clause' as test;
SELECT user_id, name, email FROM brahmand.multi_tenant_users 
WHERE tenant_id = 'acme' LIMIT 3;

SELECT 'Test 2: Join across tables with tenant filter' as test;
SELECT u.name, o.product, o.amount 
FROM brahmand.multi_tenant_users u
JOIN brahmand.multi_tenant_orders o ON u.user_id = o.user_id AND u.tenant_id = o.tenant_id
WHERE u.tenant_id = 'acme'
LIMIT 3;

SELECT 'Test 3: Graph traversal (friendships) with tenant filter' as test;
SELECT u1.name as from_user, u2.name as to_user
FROM brahmand.multi_tenant_friendships f
JOIN brahmand.multi_tenant_users u1 ON f.user_id_from = u1.user_id AND f.tenant_id = u1.tenant_id
JOIN brahmand.multi_tenant_users u2 ON f.user_id_to = u2.user_id AND f.tenant_id = u2.tenant_id
WHERE f.tenant_id = 'globex'
LIMIT 3;
