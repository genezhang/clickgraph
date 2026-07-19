WITH with_p_cte_0 AS (SELECT 
      p.full_name AS "p1_p_name", 
      p.user_id AS "p1_p_user_id"
FROM test_integration.users_test AS p
)
SELECT 
      p.p1_p_name AS "p.name", 
      post.post_title AS "post.title", 
      post2.post_title AS "post2.title"
FROM with_p_cte_0 AS p
INNER JOIN test_integration.posts_test AS t0 ON t0.author_id = p.p1_p_user_id
INNER JOIN test_integration.posts_test AS post ON post.post_id = t0.post_id
INNER JOIN test_integration.post_likes_test AS t1 ON t1.user_id = p.p1_p_user_id
INNER JOIN test_integration.posts_test AS post2 ON post2.post_id = t1.post_id
