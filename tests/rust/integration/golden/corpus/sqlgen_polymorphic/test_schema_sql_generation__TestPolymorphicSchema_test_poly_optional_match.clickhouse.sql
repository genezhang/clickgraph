SELECT 
      u.full_name AS "u.name", 
      count(t0.to_id) AS "liked"
FROM db_polymorphic.users AS u
LEFT JOIN (SELECT * FROM db_polymorphic.interactions WHERE interaction_type = 'LIKES' AND from_type = 'User' AND to_type = 'Post') AS t0 ON t0.from_id = u.user_id
GROUP BY u.full_name
