WITH optional_o1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v2.user_id AS `v2__user_id`, 
      v2.user_id AS `p2_v2_user_id`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
WHERE (v0.is_active = true AND v2.user_id > 2)
)
SELECT 
      v0.user_id AS `a.user_id`, 
      o1.p2_v2_user_id AS `b.user_id`
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
ORDER BY v0.user_id ASC NULLS LAST, o1.p2_v2_user_id ASC NULLS LAST

-- column a.user_id: Value [("a.user_id", "a.user_id")]
-- column b.user_id: Value [("b.user_id", "b.user_id")]