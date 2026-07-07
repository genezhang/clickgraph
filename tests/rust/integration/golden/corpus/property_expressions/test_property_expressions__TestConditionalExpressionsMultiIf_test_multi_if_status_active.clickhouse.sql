SELECT 
      count(*) AS "active_count"
FROM test_integration.users_expressions_test AS u
WHERE multiIf((u.is_deleted = 1), 'deleted', (u.is_banned = 1), 'banned', (u.is_active = 1), 'active', 'inactive') = 'active'
