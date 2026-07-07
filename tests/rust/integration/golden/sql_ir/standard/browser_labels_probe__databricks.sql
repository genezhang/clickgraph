SELECT DISTINCT 
      array('Post') AS `labels(n)`
FROM social.posts_bench AS n
UNION ALL 
SELECT DISTINCT 
      array('User') AS `labels(n)`
FROM social.users_bench AS n
