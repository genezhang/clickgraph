SELECT 
      u.user_id AS `u.user_id`, 
      multiIf((u.is_deleted = 1), 'deleted', (u.is_banned = 1), 'banned', (u.is_active = 1), 'active', 'inactive') AS `u.status`
FROM test_integration.users_expressions_test AS u
WHERE multiIf((u.is_deleted = 1), 'deleted', (u.is_banned = 1), 'banned', (u.is_active = 1), 'active', 'inactive') = 'banned'
