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
      v1.follower_id AS `r.from_id`, 
      v1.followed_id AS `r.to_id`, 
      v1.follow_id AS `r.edge_id`, 
      v1.follow_date AS `r.follow_date`
FROM test_integration.users_test AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.__cg_end_0 = v2.user_id
LIMIT 25
-- column r: Rel { rel_type: "FOLLOWS", from_label: "User", to_label: "User" } [("from_id", "r.from_id"), ("to_id", "r.to_id"), ("edge_id", "r.edge_id"), ("follow_date", "r.follow_date")]