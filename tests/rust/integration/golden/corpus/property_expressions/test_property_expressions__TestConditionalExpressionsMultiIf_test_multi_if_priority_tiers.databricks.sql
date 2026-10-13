SELECT 
      v0.user_id AS `u.user_id`, 
      multiIf((v0.is_deleted = 1), 'low', (v0.is_banned = 1), 'low', (v0.is_active = 1), 'high', 'medium') AS `u.priority`
FROM test_integration.users_expressions_test AS v0
WHERE v0.user_id IN (1, 3, 5)
ORDER BY v0.user_id ASC NULLS LAST

-- column u.user_id: Value [("u.user_id", "u.user_id")]
-- column u.priority: Value [("u.priority", "u.priority")]