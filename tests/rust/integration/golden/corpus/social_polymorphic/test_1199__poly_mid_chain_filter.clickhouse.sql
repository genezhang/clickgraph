SELECT 
      count(*) AS "count(*)"
FROM brahmand.interactions AS t0
INNER JOIN brahmand.interactions AS t1 ON t1.from_id = t0.to_id AND t1.interaction_type = 'FOLLOWS' AND t1.from_type = 'User' AND t1.to_type = 'User'
INNER JOIN brahmand.users_bench AS a ON t0.from_id = a.user_id
WHERE (t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User' AND (t0.to_id = 3 AND (t1.from_id <> t0.from_id OR t1.to_id <> t0.to_id OR t1.interaction_type <> t0.interaction_type OR t1.timestamp <> t0.timestamp)))
