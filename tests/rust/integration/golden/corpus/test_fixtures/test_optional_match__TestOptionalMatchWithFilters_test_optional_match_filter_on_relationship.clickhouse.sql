WITH optional_o1 AS (
SELECT 
      v0.user_id AS "v0__user_id", 
      v1.follower_id AS "v1__follower_id", 
      v1.followed_id AS "v1__followed_id", 
      v1.since AS "p2_v1_since", 
      v2.user_id AS "v2__user_id", 
      v2.name AS "p2_v2_name"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
WHERE (v0.name = 'Alice' AND v1.since > '2020')
)
SELECT 
      v0.name AS "a.name", 
      o1.p2_v2_name AS "b.name"
FROM test_integration.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
WHERE v0.name = 'Alice'
