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
), 
optional_o1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v2.user_id AS `v2__user_id`
FROM test_integration.users AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN test_integration.users AS v2 ON v1.__cg_end_0 = v2.user_id
WHERE v0.name = 'Bob'
)
SELECT 
      v0.name AS `a.name`, 
      count(o1.v2__user_id) AS `connections`
FROM test_integration.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
WHERE v0.name = 'Bob'
GROUP BY v0.name
