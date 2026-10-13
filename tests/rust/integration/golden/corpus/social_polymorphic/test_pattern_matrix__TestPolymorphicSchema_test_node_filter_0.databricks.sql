SELECT 
      v0.email_address AS `n.email`
FROM brahmand.users_bench AS v0
WHERE v0.email_address IS NOT NULL
LIMIT 10
-- column n.email: Value [("n.email", "n.email")]