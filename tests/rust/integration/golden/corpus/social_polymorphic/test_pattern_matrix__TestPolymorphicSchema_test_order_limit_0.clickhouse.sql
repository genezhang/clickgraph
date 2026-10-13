SELECT 
      v0.user_id AS "n.user_id"
FROM brahmand.users_bench AS v0
WHERE v0.user_id IS NOT NULL
ORDER BY v0.user_id DESC NULLS FIRST
LIMIT 5, 10