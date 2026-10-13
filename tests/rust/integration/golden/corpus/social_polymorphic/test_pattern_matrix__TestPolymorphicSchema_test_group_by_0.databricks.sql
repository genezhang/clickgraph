SELECT 
      v0.full_name AS `n.name`, 
      count(v0.user_id) AS `cnt`
FROM brahmand.users_bench AS v0
GROUP BY v0.full_name
ORDER BY count(v0.user_id) DESC NULLS FIRST
LIMIT 10