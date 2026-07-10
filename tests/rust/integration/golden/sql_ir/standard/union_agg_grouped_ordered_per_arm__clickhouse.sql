SELECT * FROM (
SELECT 
      u.full_name AS "name", 
      count(*) AS "c"
FROM social.users_bench AS u
INNER JOIN social.authored_bench AS t0 ON t0.user_id = u.user_id
GROUP BY u.full_name
)
ORDER BY c DESC
LIMIT 2
UNION ALL 
SELECT * FROM (
SELECT 
      u2.full_name AS "name", 
      count(*) AS "c"
FROM social.users_bench AS u2
INNER JOIN social.post_likes_bench AS t1 ON t1.user_id = u2.user_id
GROUP BY u2.full_name
)
ORDER BY c DESC
LIMIT 2
