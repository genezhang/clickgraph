SELECT 
      u.full_name AS "u.name", 
      f.full_name AS "f.name"
FROM social.users_bench AS u
INNER JOIN social.user_follows_bench AS t0 ON t0.follower_id = u.user_id
INNER JOIN social.users_bench AS f ON f.user_id = t0.followed_id
