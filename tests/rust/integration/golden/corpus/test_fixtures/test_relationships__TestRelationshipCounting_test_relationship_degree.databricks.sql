WITH v1_both AS (
SELECT 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.since AS `since`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`
FROM test_integration.follows AS e

UNION ALL
SELECT 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.since AS `since`, 
      e.followed_id AS `__cg_start_0`, 
      e.follower_id AS `__cg_end_0`
FROM test_integration.follows AS e
WHERE e.follower_id <> e.followed_id
)
SELECT 
      v0.name AS `a.name`, 
      count(DISTINCT v2.user_id) AS `connections`
FROM test_integration.users AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN test_integration.users AS v2 ON v1.__cg_end_0 = v2.user_id
GROUP BY v0.name
ORDER BY count(DISTINCT v2.user_id) DESC NULLS FIRST, v0.name ASC NULLS LAST

-- column a.name: Value [("a.name", "a.name")]
-- column connections: Value [("connections", "connections")]