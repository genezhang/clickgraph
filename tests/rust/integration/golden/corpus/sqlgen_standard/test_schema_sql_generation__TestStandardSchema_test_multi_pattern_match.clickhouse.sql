SELECT 
      u.full_name AS "u.name", 
      count(DISTINCT p.post_id) AS "posts", 
      count(DISTINCT t0.followed_id) AS "following"
FROM db_standard.posts AS p
INNER JOIN db_standard.users AS u ON u.user_id = p.user_id
INNER JOIN db_standard.user_follows AS t0 ON t0.follower_id = u.user_id
GROUP BY u.full_name
