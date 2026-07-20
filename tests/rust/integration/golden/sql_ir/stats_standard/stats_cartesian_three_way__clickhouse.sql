SELECT 
      a.full_name AS "a.name", 
      b.post_title AS "b.title", 
      c.full_name AS "c.name"
FROM social.posts_bench AS b
JOIN social.users_bench AS a ON 1 = 1
JOIN social.users_bench AS c ON 1 = 1
