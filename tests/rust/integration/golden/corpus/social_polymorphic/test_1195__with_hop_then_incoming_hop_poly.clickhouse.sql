WITH with_c_cte_0 AS (SELECT 
      t0.to_id AS "p1_c_user_id"
FROM brahmand.users_bench AS z
INNER JOIN brahmand.interactions AS t0 ON t0.from_id = z.user_id AND t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User'
)
SELECT 
      c.p1_c_user_id AS "c.user_id", 
      t1.to_id AS "n1.user_id", 
      t2.from_id AS "n2.user_id"
FROM with_c_cte_0 AS c
INNER JOIN brahmand.interactions AS t1 ON t1.from_id = c.p1_c_user_id AND t1.interaction_type = 'FOLLOWS' AND t1.from_type = 'User' AND t1.to_type = 'User'
INNER JOIN brahmand.interactions AS t2 ON t2.to_id = t1.to_id AND t2.interaction_type = 'FOLLOWS' AND t2.from_type = 'User' AND t2.to_type = 'User'
WHERE (t2.from_id <> t1.from_id OR t2.to_id <> t1.to_id OR t2.interaction_type <> t1.interaction_type OR t2.timestamp <> t1.timestamp)
