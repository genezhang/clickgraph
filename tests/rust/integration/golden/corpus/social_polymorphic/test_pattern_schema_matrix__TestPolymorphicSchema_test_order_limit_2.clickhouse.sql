SELECT 
      n.full_name AS "n.name"
FROM brahmand.users_bench AS n
WHERE n.full_name IS NOT NULL
ORDER BY n.full_name DESC NULLS FIRST
LIMIT 5, 10