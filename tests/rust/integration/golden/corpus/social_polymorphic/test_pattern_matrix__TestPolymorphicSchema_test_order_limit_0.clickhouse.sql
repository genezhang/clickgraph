SELECT 
      n.user_id AS "n.user_id"
FROM brahmand.users_bench AS n
WHERE n.user_id IS NOT NULL
ORDER BY n.user_id DESC NULLS FIRST
LIMIT 5, 10