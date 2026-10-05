SELECT 
      count(*) AS "count(*)"
FROM brahmand.interactions AS r2
INNER JOIN brahmand.interactions AS r1 ON r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r1.to_id = r2.from_id
INNER JOIN brahmand.users_bench AS a ON a.user_id = r1.from_id
WHERE (((r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User') AND r2.to_type = 'User') AND (r2.to_id = 3 AND NOT (((r1.from_id = r2.from_id AND r1.to_id = r2.to_id) AND r1.interaction_type = r2.interaction_type) AND r1.timestamp = r2.timestamp)))
