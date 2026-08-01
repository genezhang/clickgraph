SELECT 
      u.full_name AS "u.name", 
      count(DISTINCT p.post_id) AS "posts", 
      count(DISTINCT t0.user_id) AS "likers"
FROM db_standard.users AS u
LEFT JOIN db_standard.posts AS p ON p.user_id = u.user_id
LEFT JOIN db_standard.post_likes AS t0 ON t0.post_id = p.post_id
GROUP BY u.full_name
