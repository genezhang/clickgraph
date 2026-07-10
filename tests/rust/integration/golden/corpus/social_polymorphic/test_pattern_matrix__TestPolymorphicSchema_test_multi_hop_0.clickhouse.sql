SELECT 
      a.email_address AS "a.email", 
      c.email_address AS "c.email"
FROM brahmand.users_bench AS a
INNER JOIN brahmand.interactions AS r1 ON r1.from_id = a.user_id AND r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User'
INNER JOIN brahmand.interactions AS r2 ON r2.from_id = r1.to_id AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
INNER JOIN brahmand.users_bench AS c ON c.user_id = r2.to_id
LIMIT 5