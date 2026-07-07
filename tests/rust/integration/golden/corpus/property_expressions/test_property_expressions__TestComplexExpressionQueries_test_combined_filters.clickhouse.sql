SELECT 
      u.user_id AS "u.user_id", 
      concat(u.first_name, ' ', u.last_name) AS "u.full_name", 
      if((u.score >= 1000), 'gold', if((u.score >= 500), 'silver', 'bronze')) AS "u.tier", 
      multiIf((u.is_deleted = 1), 'deleted', (u.is_banned = 1), 'banned', (u.is_active = 1), 'active', 'inactive') AS "u.status"
FROM test_integration.users_expressions_test AS u
WHERE ((if((u.score >= 1000), 'gold', if((u.score >= 500), 'silver', 'bronze')) = 'gold' AND multiIf((u.is_deleted = 1), 'deleted', (u.is_banned = 1), 'banned', (u.is_active = 1), 'active', 'inactive') = 'active') AND u.is_premium = true)
ORDER BY u.user_id ASC
