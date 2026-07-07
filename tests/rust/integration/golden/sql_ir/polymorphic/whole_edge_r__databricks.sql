SELECT 
      r.from_id AS `r.from_id`, 
      r.to_id AS `r.to_id`, 
      r.timestamp AS `r.created_at`, 
      r.interaction_weight AS `r.weight`
FROM brahmand.interactions AS r
WHERE r.interaction_type = 'FOLLOWS' AND r.from_type = 'User' AND r.to_type = 'User'
