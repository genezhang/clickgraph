SELECT 
      count(*) AS "active_count"
FROM test_integration.users_expressions_test AS v0
WHERE multiIf((v0.is_deleted = 1), 'deleted', (v0.is_banned = 1), 'banned', (v0.is_active = 1), 'active', 'inactive') = 'active'

-- column active_count: Value [("active_count", "active_count")]