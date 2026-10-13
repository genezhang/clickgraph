WITH optional_o1 AS (
SELECT 
      v1.follower_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id"
FROM test_integration.follows AS v1
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
), 
with_w2 AS (
SELECT 
      v0.user_id AS "v3__user_id", 
      v0.name AS "p2_v3_name", 
      count(o1.v2__user_id) AS "v4"
FROM test_integration.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
GROUP BY v0.user_id, v0.name
)
SELECT 
      w2.p2_v3_name AS "a.name", 
      CASE WHEN w2.v4 = 0 THEN 'No follows' WHEN w2.v4 = 1 THEN 'One follow' ELSE 'Multiple follows' END AS "follow_status"
FROM with_w2 AS w2
ORDER BY w2.p2_v3_name ASC
