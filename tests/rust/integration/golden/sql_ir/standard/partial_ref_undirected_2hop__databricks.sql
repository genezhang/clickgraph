SELECT 
      a.full_name AS `a.name`, 
      c.full_name AS `c.name`
FROM social.users_bench AS a
INNER JOIN social.user_follows_bench AS t0 ON t0.follower_id = a.user_id
INNER JOIN social.user_follows_bench AS t1 ON t1.follower_id = t0.followed_id
INNER JOIN social.users_bench AS c ON c.user_id = t1.followed_id
WHERE NOT (t1.follower_id = t0.follower_id AND t1.followed_id = t0.followed_id)
UNION ALL 
SELECT 
      a.full_name AS `a.name`, 
      c.full_name AS `c.name`
FROM social.users_bench AS b
INNER JOIN social.user_follows_bench AS t0 ON t0.follower_id = b.user_id
INNER JOIN social.users_bench AS a ON a.user_id = t0.followed_id
INNER JOIN social.user_follows_bench AS t1 ON t1.follower_id = b.user_id
INNER JOIN social.users_bench AS c ON c.user_id = t1.followed_id
WHERE NOT (t1.follower_id = t0.follower_id AND t1.followed_id = t0.followed_id)
UNION ALL 
SELECT 
      a.full_name AS `a.name`, 
      c.full_name AS `c.name`
FROM social.users_bench AS a
INNER JOIN social.user_follows_bench AS t0 ON t0.follower_id = a.user_id
INNER JOIN social.user_follows_bench AS t1 ON t1.followed_id = t0.followed_id
INNER JOIN social.users_bench AS c ON c.user_id = t1.follower_id
WHERE NOT (t1.follower_id = t0.follower_id AND t1.followed_id = t0.followed_id)
UNION ALL 
SELECT 
      a.full_name AS `a.name`, 
      c.full_name AS `c.name`
FROM social.users_bench AS b
INNER JOIN social.user_follows_bench AS t0 ON t0.follower_id = b.user_id
INNER JOIN social.users_bench AS a ON a.user_id = t0.followed_id
INNER JOIN social.user_follows_bench AS t1 ON t1.followed_id = b.user_id
INNER JOIN social.users_bench AS c ON c.user_id = t1.follower_id
WHERE NOT (t1.follower_id = t0.follower_id AND t1.followed_id = t0.followed_id)
