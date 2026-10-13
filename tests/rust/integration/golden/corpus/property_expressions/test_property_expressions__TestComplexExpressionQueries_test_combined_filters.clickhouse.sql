SELECT 
      v0.user_id AS "u.user_id", 
      concat(v0.first_name, ' ', v0.last_name) AS "u.full_name", 
      if((v0.score >= 1000), 'gold', if((v0.score >= 500), 'silver', 'bronze')) AS "u.tier", 
      multiIf((v0.is_deleted = 1), 'deleted', (v0.is_banned = 1), 'banned', (v0.is_active = 1), 'active', 'inactive') AS "u.status"
FROM test_integration.users_expressions_test AS v0
WHERE ((if((v0.score >= 1000), 'gold', if((v0.score >= 500), 'silver', 'bronze')) = 'gold' AND multiIf((v0.is_deleted = 1), 'deleted', (v0.is_banned = 1), 'banned', (v0.is_active = 1), 'active', 'inactive') = 'active') AND v0.is_premium = true)
ORDER BY v0.user_id ASC
