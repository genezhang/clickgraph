SELECT DISTINCT 
      ['Post'] AS "labels(n)"
FROM social.posts_bench AS n
UNION ALL 
SELECT DISTINCT 
      ['User'] AS "labels(n)"
FROM social.users_bench AS n
