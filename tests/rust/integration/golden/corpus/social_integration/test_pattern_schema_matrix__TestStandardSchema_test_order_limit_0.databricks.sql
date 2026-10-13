SELECT 
      v0.user_id AS `n.user_id`
FROM test_integration.users_test AS v0
WHERE v0.user_id IS NOT NULL
ORDER BY v0.user_id DESC NULLS FIRST
LIMIT 10 OFFSET 5
-- column n.user_id: Value [("n.user_id", "n.user_id")]