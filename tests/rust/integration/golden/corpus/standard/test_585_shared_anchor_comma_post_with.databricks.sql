WITH with_w1 AS (
SELECT 
      v0.user_id AS `v1__user_id`, 
      v0.full_name AS `p2_v1_name`
FROM test_integration.users_test AS v0
)
SELECT 
      w1.p2_v1_name AS `p.name`, 
      v3.post_title AS `post.title`, 
      v5.post_title AS `post2.title`
FROM with_w1 AS w1
JOIN test_integration.posts_test AS v2 ON v2.author_id = w1.v1__user_id
JOIN test_integration.posts_test AS v3 ON v2.post_id = v3.post_id
JOIN test_integration.post_likes_test AS v4 ON v4.user_id = w1.v1__user_id
JOIN test_integration.posts_test AS v5 ON v4.post_id = v5.post_id

-- column p.name: Value [("p.name", "p.name")]
-- column post.title: Value [("post.title", "post.title")]
-- column post2.title: Value [("post2.title", "post2.title")]