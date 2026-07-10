SELECT 
      n.full_name AS `n.name`, 
      count(n.user_id) AS `cnt`
FROM brahmand.users_bench AS n
GROUP BY n.full_name
ORDER BY cnt DESC
LIMIT 10