WITH with_w1 AS (
SELECT 
      v2.user_id AS `v3__user_id`, 
      v2.country AS `p2_v3_country`, 
      v2.email_address AS `p2_v3_email`, 
      v2.full_name AS `p2_v3_name`, 
      v0.user_id AS `v4`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
WHERE v2.country = 'USA'
)
SELECT 
      w1.p2_v3_name AS `f.name`, 
      w1.p2_v3_email AS `f.email`
FROM with_w1 AS w1

-- column f.name: Value [("f.name", "f.name")]
-- column f.email: Value [("f.email", "f.email")]