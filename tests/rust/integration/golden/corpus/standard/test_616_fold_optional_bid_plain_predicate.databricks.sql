WITH optional_o1 AS (
SELECT 
      v1.follower_id AS `v0__user_id`, 
      v2.user_id AS `v2__user_id`, 
      v2.country AS `p2_v2_country`, 
      v2.id AS `p2_v2_id`
FROM test_integration.user_follows_test AS v1
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
WHERE v2.country = 'USA'
)
SELECT 
      v0.id AS `a.id`, 
      o1.p2_v2_id AS `b.id`
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id

-- column a.id: Value [("a.id", "a.id")]
-- column b.id: Value [("b.id", "b.id")]