WITH optional_o1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v4.user_id AS `v4__user_id`, 
      v4.user_id AS `p2_v4_user_id`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v3 ON v3.follower_id = v0.user_id
JOIN test_integration.users_test AS v4 ON v3.followed_id = v4.user_id
WHERE (v0.is_active = true AND v0.is_active = true)
)
SELECT 
      v0.user_id AS `a.user_id`, 
      v2.post_id AS `z.post_id`, 
      o1.p2_v4_user_id AS `b.user_id`
FROM test_integration.users_test AS v0
JOIN test_integration.posts_test AS v1 ON v1.author_id = v0.user_id
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
WHERE v0.is_active = true
ORDER BY v0.user_id ASC NULLS LAST, v2.post_id ASC NULLS LAST, o1.p2_v4_user_id ASC NULLS LAST

-- column a.user_id: Value [("a.user_id", "a.user_id")]
-- column z.post_id: Value [("z.post_id", "z.post_id")]
-- column b.user_id: Value [("b.user_id", "b.user_id")]