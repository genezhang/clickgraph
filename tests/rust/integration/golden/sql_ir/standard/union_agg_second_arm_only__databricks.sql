SELECT 
      u.user_id AS `v`
FROM social.users_bench AS u
UNION ALL 
SELECT 
      count(r.follower_id) AS `v`
FROM social.user_follows_bench AS r
