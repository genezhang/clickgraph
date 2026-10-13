SELECT 
      v0.email_address AS `n.email`, 
      count(v0.user_id) AS `cnt`
FROM brahmand.users_bench AS v0
GROUP BY v0.email_address
ORDER BY count(v0.user_id) DESC NULLS FIRST
LIMIT 10