SELECT 
      r.timestamp AS "r.created_at", 
      neighbor.full_name AS "neighbor.name"
FROM brahmand.users_bench AS u
INNER JOIN brahmand.interactions AS r ON r.from_id = u.user_id AND r.interaction_type = 'FOLLOWS' AND r.from_type = 'User' AND r.to_type = 'User'
INNER JOIN brahmand.users_bench AS neighbor ON neighbor.user_id = r.to_id
WHERE u.user_id = 1
LIMIT 5