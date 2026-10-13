WITH v1_rels AS (
SELECT 
      'TEST_FOLLOWS' AS "__cg_type", 
      'TestUser' AS "__cg_from_label", 
      'TestUser' AS "__cg_to_label", 
      e.follower_id AS "__cg_rid_0", 
      e.followed_id AS "__cg_rid_1", 
      e.follower_id AS "__cg_from_0", 
      e.followed_id AS "__cg_to_0", 
      e.follower_id AS "__cg_start_0", 
      e.followed_id AS "__cg_end_0", 
      'TestUser' AS "__cg_start_label", 
      'TestUser' AS "__cg_end_label", 
      concat('TEST_FOLLOWS:TestUser:TestUser:', toString(tuple(e.follower_id, e.followed_id))) AS "__cg_key"
FROM test_integration.follows AS e

UNION ALL
SELECT 
      'TEST_FRIENDS_WITH' AS "__cg_type", 
      'TestUser' AS "__cg_from_label", 
      'TestUser' AS "__cg_to_label", 
      e.user_id_1 AS "__cg_rid_0", 
      e.user_id_2 AS "__cg_rid_1", 
      e.user_id_1 AS "__cg_from_0", 
      e.user_id_2 AS "__cg_to_0", 
      e.user_id_1 AS "__cg_start_0", 
      e.user_id_2 AS "__cg_end_0", 
      'TestUser' AS "__cg_start_label", 
      'TestUser' AS "__cg_end_label", 
      concat('TEST_FRIENDS_WITH:TestUser:TestUser:', toString(tuple(e.user_id_1, e.user_id_2))) AS "__cg_key"
FROM test_integration.friendships AS e
)
SELECT 
      count(v1.__cg_type) AS "total"
FROM test_integration.users AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN test_integration.users AS v2 ON v1.__cg_end_0 = v2.user_id

-- column total: Value [("total", "total")]