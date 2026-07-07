SELECT 
      u.user_id AS `u.user_id`, 
      multiIf((u.is_deleted = 1), 'low', (u.is_banned = 1), 'low', (u.is_active = 1), 'high', 'medium') AS `u.priority`
FROM test_integration.users_expressions_test AS u
WHERE u.user_id IN (1, 3, 5)
ORDER BY u.user_id ASC
