SELECT 
      u.full_name AS `u.name`, 
      f.full_name AS `f.name`
FROM db_polymorphic.users AS u
INNER JOIN db_polymorphic.interactions AS t0 ON t0.from_id = u.user_id AND t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User'
INNER JOIN db_polymorphic.users AS f ON f.user_id = t0.to_id
UNION ALL 
SELECT 
      u.full_name AS `u.name`, 
      f.full_name AS `f.name`
FROM db_polymorphic.users AS f
INNER JOIN db_polymorphic.interactions AS t0 ON t0.from_id = f.user_id AND t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User'
INNER JOIN db_polymorphic.users AS u ON u.user_id = t0.to_id
