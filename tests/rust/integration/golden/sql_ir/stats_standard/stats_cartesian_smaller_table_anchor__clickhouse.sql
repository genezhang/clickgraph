SELECT 
      a.full_name AS "a.name", 
      b.post_title AS "b.title"
FROM social.posts_bench AS b
JOIN social.users_bench AS a ON 1 = 1
