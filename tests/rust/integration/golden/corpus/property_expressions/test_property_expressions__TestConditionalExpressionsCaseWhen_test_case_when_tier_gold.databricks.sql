SELECT 
      u.user_id AS `u.user_id`, 
      concat(u.first_name, ' ', u.last_name) AS `u.full_name`, 
      if((u.score >= 1000), 'gold', if((u.score >= 500), 'silver', 'bronze')) AS `u.tier`
FROM test_integration.users_expressions_test AS u
WHERE u.score >= 1000
ORDER BY u.user_id ASC
