SELECT `gt` AS `gt`, count(*) AS `c` FROM (
SELECT 
      CAST(v.user_id > 2 AS BOOLEAN) AS `gt`,
      v.user_id AS `v.user_id`
FROM social.users_bench AS u
INNER JOIN social.user_follows_bench AS t0 ON t0.follower_id = u.user_id
INNER JOIN social.users_bench AS v ON v.user_id = t0.followed_id
UNION ALL 
SELECT 
      CAST(v.user_id > 2 AS BOOLEAN) AS `gt`,
      v.user_id AS `v.user_id`
FROM social.users_bench AS v
INNER JOIN social.user_follows_bench AS t0 ON t0.follower_id = v.user_id
) AS __union
GROUP BY `gt`
ORDER BY `gt` ASC NULLS LAST
