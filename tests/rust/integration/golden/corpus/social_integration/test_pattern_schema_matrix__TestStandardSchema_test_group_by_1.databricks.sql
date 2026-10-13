SELECT 
      v0.user_id AS `n.user_id`, 
      count(v0.user_id) AS `cnt`
FROM test_integration.users_test AS v0
GROUP BY v0.user_id
ORDER BY count(v0.user_id) DESC NULLS FIRST
LIMIT 10
-- column n.user_id: Value [("n.user_id", "n.user_id")]
-- column cnt: Value [("cnt", "cnt")]