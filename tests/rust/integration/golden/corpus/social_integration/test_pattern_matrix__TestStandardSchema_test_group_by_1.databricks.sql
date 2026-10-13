SELECT 
      v0.email_address AS `n.email`, 
      count(v0.user_id) AS `cnt`
FROM test_integration.users_test AS v0
GROUP BY v0.email_address
ORDER BY count(v0.user_id) DESC NULLS FIRST
LIMIT 10
-- column n.email: Value [("n.email", "n.email")]
-- column cnt: Value [("cnt", "cnt")]