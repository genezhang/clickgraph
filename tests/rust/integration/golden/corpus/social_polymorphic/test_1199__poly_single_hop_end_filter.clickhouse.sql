SELECT 
      a.full_name AS "a.name"
FROM brahmand.interactions AS t0
INNER JOIN brahmand.users_bench AS a ON t0.from_id = a.user_id
WHERE (t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User' AND t0.to_id = 3)
