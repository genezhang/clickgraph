WITH v1_both AS (
SELECT 
      e.follow_date AS `follow_date`, 
      e.follow_id AS `follow_id`, 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      e.follow_date AS `follow_date`, 
      e.follow_id AS `follow_id`, 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.followed_id AS `__cg_start_0`, 
      e.follower_id AS `__cg_end_0`
FROM test_integration.user_follows_test AS e
WHERE e.follower_id <> e.followed_id
)
SELECT 
      v2.user_id AS `neighbor.user_id`, 
      v2.full_name AS `neighbor.name`
FROM test_integration.users_test AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.__cg_end_0 = v2.user_id
WHERE v0.user_id = 1
LIMIT 10
-- column neighbor.user_id: Value [("neighbor.user_id", "neighbor.user_id")]
-- column neighbor.name: Value [("neighbor.name", "neighbor.name")]