SELECT 
      neighbor.user_id AS `neighbor.user_id`, 
      neighbor.full_name AS `neighbor.name`
FROM brahmand.users_bench AS u
INNER JOIN brahmand.interactions AS t0 ON t0.from_id = u.user_id AND t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User'
INNER JOIN brahmand.users_bench AS neighbor ON neighbor.user_id = t0.to_id
WHERE u.user_id = 1
LIMIT 10