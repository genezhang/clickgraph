WITH optional_o1 AS (
SELECT 
      v1.follower_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id"
FROM test_integration.follows AS v1
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
), 
optional_o2 AS (
SELECT 
      v4.followed_id AS "v0__user_id", 
      v3.user_id AS "v3__user_id"
FROM test_integration.users AS v3
JOIN test_integration.follows AS v4 ON v4.follower_id = v3.user_id
)
SELECT 
      v0.name AS "n.name", 
      count(DISTINCT o1.v2__user_id) AS "following", 
      count(DISTINCT o2.v3__user_id) AS "followers"
FROM test_integration.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
LEFT JOIN optional_o2 AS o2 ON v0.user_id = o2.v0__user_id
GROUP BY v0.name
ORDER BY v0.name ASC
