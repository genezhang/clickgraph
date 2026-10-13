WITH optional_o1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v2.user_id AS `v2__user_id`, 
      v2.name AS `p2_v2_name`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
WHERE v0.name = 'Alice'
), 
with_w2 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v0.name AS `p2_v0_name`, 
      o1.v2__user_id AS `v2__user_id`, 
      o1.p2_v2_name AS `p2_v2_name`
FROM test_integration.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
WHERE v0.name = 'Alice'
), 
optional_d3 AS (
SELECT DISTINCT 
      w2.p2_v2_name AS `p2_v2_name`, 
      w2.v0__user_id AS `v0__user_id`, 
      w2.v2__user_id AS `v2__user_id`
FROM with_w2 AS w2
), 
optional_o4 AS (
SELECT 
      d3.v0__user_id AS `v0__user_id`, 
      d3.v2__user_id AS `v2__user_id`, 
      v4.user_id AS `v4__user_id`, 
      v4.name AS `p2_v4_name`
FROM optional_d3 AS d3
JOIN test_integration.follows AS v3 ON v3.follower_id = d3.v0__user_id
JOIN test_integration.users AS v4 ON v3.followed_id = v4.user_id
WHERE v4.name <> d3.p2_v2_name
)
SELECT 
      w2.p2_v0_name AS `a.name`, 
      w2.p2_v2_name AS `b.name`, 
      o4.p2_v4_name AS `c.name`
FROM with_w2 AS w2
LEFT JOIN optional_o4 AS o4 ON w2.v0__user_id = o4.v0__user_id AND (w2.v2__user_id = o4.v2__user_id OR (w2.v2__user_id IS NULL AND o4.v2__user_id IS NULL))
ORDER BY w2.p2_v2_name ASC NULLS LAST, o4.p2_v4_name ASC NULLS LAST
