SELECT 
      a.full_name AS "a.name", 
      r.to_id AS "b.user_id"
FROM brahmand.users_bench AS a
INNER JOIN brahmand.interactions AS r ON r.from_id = a.user_id AND r.interaction_type = 'LIKES' AND r.from_type = 'User' AND r.to_type = 'User'
LIMIT 10