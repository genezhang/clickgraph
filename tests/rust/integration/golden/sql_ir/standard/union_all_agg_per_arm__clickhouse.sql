SELECT 
      count(r.user_id) AS "c"
FROM social.authored_bench AS r
UNION ALL 
SELECT 
      count(r2.follower_id) AS "c"
FROM social.user_follows_bench AS r2
