SELECT 
      n.user_id AS "n.user_id"
FROM brahmand.users_bench AS n
WHERE n.user_id IS NOT NULL
LIMIT 10