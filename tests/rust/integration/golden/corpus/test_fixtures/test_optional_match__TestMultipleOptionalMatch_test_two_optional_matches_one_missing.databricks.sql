WITH optional_o1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v2.user_id AS `v2__user_id`, 
      v2.name AS `p2_v2_name`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
WHERE v0.name = 'Eve'
), 
optional_o2 AS (
SELECT 
      v3.follower_id AS `v2__user_id`, 
      v4.user_id AS `v4__user_id`, 
      v4.name AS `p2_v4_name`
FROM test_integration.follows AS v3
JOIN test_integration.users AS v4 ON v3.followed_id = v4.user_id
WHERE v3.follower_id IS NOT NULL
)
SELECT 
      v0.name AS `a.name`, 
      o1.p2_v2_name AS `b.name`, 
      o2.p2_v4_name AS `c.name`
FROM test_integration.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
LEFT JOIN optional_o2 AS o2 ON o1.v2__user_id = o2.v2__user_id
WHERE v0.name = 'Eve'
