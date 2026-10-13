SELECT 
      v0.user_id AS "u.user_id", 
      if((v0.score >= 1000), 'gold', if((v0.score >= 500), 'silver', 'bronze')) AS "u.tier"
FROM test_integration.users_expressions_test AS v0
WHERE (v0.score >= 500 AND v0.score < 1000)
ORDER BY v0.user_id ASC

-- column u.user_id: Value [("u.user_id", "u.user_id")]
-- column u.tier: Value [("u.tier", "u.tier")]