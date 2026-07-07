SELECT 
      n.email_address AS `n.email`
FROM brahmand.users_bench AS n
WHERE n.email_address IS NOT NULL
ORDER BY n.email_address DESC
LIMIT 10 OFFSET 5