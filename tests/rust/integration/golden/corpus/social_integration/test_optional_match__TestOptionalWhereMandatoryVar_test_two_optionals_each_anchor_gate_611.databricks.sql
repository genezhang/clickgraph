WITH optional_o1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v2.user_id AS `v2__user_id`, 
      v2.user_id AS `p2_v2_user_id`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
WHERE v0.is_active = true
), 
optional_o2 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v4.post_id AS `v4__post_id`, 
      v4.post_id AS `p2_v4_post_id`
FROM test_integration.users_test AS v0
JOIN test_integration.post_likes_test AS v3 ON v3.user_id = v0.user_id
JOIN test_integration.posts_test AS v4 ON v3.post_id = v4.post_id
WHERE v0.country = 'US'
)
SELECT 
      v0.user_id AS `a.user_id`, 
      o1.p2_v2_user_id AS `b.user_id`, 
      o2.p2_v4_post_id AS `p.post_id`
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
LEFT JOIN optional_o2 AS o2 ON v0.user_id = o2.v0__user_id
ORDER BY v0.user_id ASC NULLS LAST, o1.p2_v2_user_id ASC NULLS LAST, o2.p2_v4_post_id ASC NULLS LAST
