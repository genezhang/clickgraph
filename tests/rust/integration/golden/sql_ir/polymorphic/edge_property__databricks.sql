SELECT 
      u.full_name AS `u.name`, 
      r.interaction_weight AS `r.weight`
FROM brahmand.users_bench AS u
INNER JOIN brahmand.interactions AS r ON r.from_id = u.user_id AND r.interaction_type = 'LIKES' AND r.from_type = 'User' AND r.to_type = 'Post'
