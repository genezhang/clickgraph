SELECT 
      r.interaction_type AS `type(r)`, 
      a.user_id AS `id(a)`, 
      array('User') AS `labels(a)`
FROM brahmand.users_bench AS a
INNER JOIN brahmand.interactions AS r ON r.from_id = a.user_id AND r.interaction_type = 'LIKES' AND r.from_type = 'User' AND r.to_type = 'User'
LIMIT 5