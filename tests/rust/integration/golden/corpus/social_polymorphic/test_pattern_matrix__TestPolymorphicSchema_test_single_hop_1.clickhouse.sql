SELECT 
      a.email_address AS "a.email", 
      b.email_address AS "b.email"
FROM brahmand.users_bench AS a
INNER JOIN brahmand.interactions AS r ON r.from_id = a.user_id AND r.interaction_type = 'LIKES' AND r.from_type = 'User' AND r.to_type = 'User'
INNER JOIN brahmand.users_bench AS b ON b.user_id = r.to_id
LIMIT 10