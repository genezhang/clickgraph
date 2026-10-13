WITH optional_o1 AS (
SELECT 
      v0.user_id AS "v0__user_id", 
      v0.name AS "p2_v0_name", 
      v2.user_id AS "v2__user_id", 
      v2.name AS "p2_v2_name"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
WHERE v0.name = 'Eve'
), 
with_w2 AS (
SELECT 
      1 AS "__row"
)
SELECT 
      o1.p2_v0_name AS "a.name", 
      o1.p2_v2_name AS "b.name", 
      v3.name AS "x.name"
FROM with_w2 AS w2
LEFT JOIN optional_o1 AS o1 ON 1 = 1
CROSS JOIN test_integration.users AS v3
WHERE v3.name = 'Alice'

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]
-- column x.name: Value [("x.name", "x.name")]