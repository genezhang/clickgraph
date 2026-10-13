SELECT 
      v0.email_address AS `n.email`
FROM brahmand.users_bench AS v0
WHERE v0.email_address IS NOT NULL
ORDER BY v0.email_address DESC NULLS FIRST
LIMIT 10 OFFSET 5