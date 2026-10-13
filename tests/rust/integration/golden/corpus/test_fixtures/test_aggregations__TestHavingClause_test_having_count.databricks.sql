WITH with_w1 AS (
SELECT 
      v0.user_id AS `v3__user_id`, 
      v0.name AS `p2_v3_name`, 
      count(v2.user_id) AS `v4`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
GROUP BY v0.user_id, v0.name
HAVING count(v2.user_id) > 1
)
SELECT 
      w1.p2_v3_name AS `a.name`, 
      w1.v4 AS `follows`
FROM with_w1 AS w1
ORDER BY w1.p2_v3_name ASC NULLS LAST
