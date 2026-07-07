SELECT 
      u.full_name AS "u.name", 
      count(DISTINCT p.post_id) AS "posts", 
      count(DISTINCT t0.user_id) AS "likers"
FROM db_standard.posts AS p
LEFT JOIN db_standard.post_likes AS t0 ON t0.post_id = p.post_id
LEFT JOIN db_standard.users AS u ON u.user_id = p.user_id
GROUP BY u.full_name
