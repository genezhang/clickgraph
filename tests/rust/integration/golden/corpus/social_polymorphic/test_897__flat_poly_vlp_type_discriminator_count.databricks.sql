SELECT 
      count(*) AS `c`
FROM brahmand.users_bench AS a
INNER JOIN brahmand.interactions AS r1 ON a.user_id = r1.from_id AND r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User'
INNER JOIN brahmand.interactions AS r2 ON r1.to_id = r2.from_id AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
WHERE NOT (((r1.from_id = r2.from_id AND r1.to_id = r2.to_id) AND r1.interaction_type = r2.interaction_type) AND r1.timestamp = r2.timestamp)
