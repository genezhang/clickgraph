WITH v2_labels AS (
SELECT 
      'TestProduct' AS `__cg_label`, 
      e.product_id AS `__cg_id_0`
FROM test_integration.products AS e

UNION ALL
SELECT 
      'TestUser' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`
FROM test_integration.users AS e
), 
v1_rels AS (
SELECT 
      'TEST_FOLLOWS' AS `__cg_type`, 
      'TestUser' AS `__cg_from_label`, 
      'TestUser' AS `__cg_to_label`, 
      e.follower_id AS `__cg_rid_0`, 
      e.followed_id AS `__cg_rid_1`, 
      e.follower_id AS `__cg_from_0`, 
      e.followed_id AS `__cg_to_0`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`, 
      'TestUser' AS `__cg_start_label`, 
      'TestUser' AS `__cg_end_label`, 
      concat('TEST_FOLLOWS:TestUser:TestUser:', to_json(named_struct('_1', CAST(e.follower_id AS STRING), '_2', CAST(e.followed_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM test_integration.follows AS e

UNION ALL
SELECT 
      'TEST_FRIENDS_WITH' AS `__cg_type`, 
      'TestUser' AS `__cg_from_label`, 
      'TestUser' AS `__cg_to_label`, 
      e.user_id_1 AS `__cg_rid_0`, 
      e.user_id_2 AS `__cg_rid_1`, 
      e.user_id_1 AS `__cg_from_0`, 
      e.user_id_2 AS `__cg_to_0`, 
      e.user_id_1 AS `__cg_start_0`, 
      e.user_id_2 AS `__cg_end_0`, 
      'TestUser' AS `__cg_start_label`, 
      'TestUser' AS `__cg_end_label`, 
      concat('TEST_FRIENDS_WITH:TestUser:TestUser:', to_json(named_struct('_1', CAST(e.user_id_1 AS STRING), '_2', CAST(e.user_id_2 AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM test_integration.friendships AS e

UNION ALL
SELECT 
      'TEST_PURCHASED' AS `__cg_type`, 
      'TestUser' AS `__cg_from_label`, 
      'TestProduct' AS `__cg_to_label`, 
      e.user_id AS `__cg_rid_0`, 
      e.product_id AS `__cg_rid_1`, 
      e.user_id AS `__cg_from_0`, 
      e.product_id AS `__cg_to_0`, 
      e.user_id AS `__cg_start_0`, 
      e.product_id AS `__cg_end_0`, 
      'TestUser' AS `__cg_start_label`, 
      'TestProduct' AS `__cg_end_label`, 
      concat('TEST_PURCHASED:TestUser:TestProduct:', to_json(named_struct('_1', CAST(e.user_id AS STRING), '_2', CAST(e.product_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM test_integration.purchases AS e
)
SELECT 
      count(v1.__cg_type) AS `total`
FROM test_integration.users AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0

-- column total: Value [("total", "total")]