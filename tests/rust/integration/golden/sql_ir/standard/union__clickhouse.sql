SELECT 
      u.full_name AS "x"
FROM social.users_bench AS u
UNION DISTINCT 
SELECT 
      p.post_title AS "x"
FROM social.posts_bench AS p
