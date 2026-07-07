SELECT 
      a.full_name AS "a.name", 
      b.full_name AS "b.name"
FROM brahmand.users_bench AS a
INNER JOIN brahmand.interactions AS t0 ON t0.from_id = a.user_id AND t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User'
INNER JOIN brahmand.users_bench AS b ON b.user_id = t0.to_id
UNION ALL 
SELECT 
      a.full_name AS "a.name", 
      b.full_name AS "b.name"
FROM brahmand.users_bench AS b
INNER JOIN brahmand.interactions AS t0 ON t0.from_id = b.user_id AND t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User'
INNER JOIN brahmand.users_bench AS a ON a.user_id = t0.to_id
