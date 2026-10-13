SELECT 
      v0.user_id AS "u.user_id", 
      concat(v0.first_name, ' ', v0.last_name) AS "u.full_name", 
      if((v0.score >= 1000), 'gold', if((v0.score >= 500), 'silver', 'bronze')) AS "u.tier"
FROM test_integration.users_expressions_test AS v0
WHERE v0.score >= 1000
ORDER BY v0.user_id ASC
