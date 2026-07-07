SELECT 
      u.full_name AS "u.name", 
      count(t0.to_id) AS "posts"
FROM db_polymorphic.users AS u
INNER JOIN db_polymorphic.interactions AS t0 ON t0.from_id = u.user_id AND t0.interaction_type = 'AUTHORED' AND t0.from_type = 'User' AND t0.to_type = 'Post'
GROUP BY u.full_name
