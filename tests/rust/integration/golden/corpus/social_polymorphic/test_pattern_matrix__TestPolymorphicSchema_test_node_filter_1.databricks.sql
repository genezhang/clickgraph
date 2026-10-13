SELECT 
      v0.user_id AS `n.user_id`
FROM brahmand.users_bench AS v0
WHERE v0.user_id IS NOT NULL
LIMIT 10
-- column n.user_id: Value [("n.user_id", "n.user_id")]