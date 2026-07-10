SELECT 
      count(*) AS `c`
FROM social.authored_bench AS r
UNION DISTINCT 
SELECT 
      count(*) AS `c`
FROM social.user_follows_bench AS r2
