SELECT 
      v0.full_name AS "n.name"
FROM brahmand.users_bench AS v0
WHERE v0.full_name IS NOT NULL
ORDER BY v0.full_name DESC NULLS FIRST
LIMIT 5, 10
-- column n.name: Value [("n.name", "n.name")]