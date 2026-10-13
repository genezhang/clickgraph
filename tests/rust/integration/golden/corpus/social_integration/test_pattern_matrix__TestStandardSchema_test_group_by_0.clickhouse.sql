SELECT 
      v0.is_active AS "n.is_active", 
      count(v0.user_id) AS "cnt"
FROM test_integration.users_test AS v0
GROUP BY v0.is_active
ORDER BY count(v0.user_id) DESC NULLS FIRST
LIMIT 10
-- column n.is_active: Value [("n.is_active", "n.is_active")]
-- column cnt: Value [("cnt", "cnt")]