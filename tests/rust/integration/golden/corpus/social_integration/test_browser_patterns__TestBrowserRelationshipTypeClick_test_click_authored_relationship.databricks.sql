WITH v0_labels AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`
FROM test_integration.users_test AS e
), 
v2_labels AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`
FROM test_integration.users_test AS e
), 
v1_rels AS (
SELECT 
      'AUTHORED' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.author_id AS `__cg_rid_0`, 
      e.post_id AS `__cg_rid_1`, 
      e.author_id AS `__cg_from_0`, 
      e.post_id AS `__cg_to_0`, 
      e.post_id AS `__cg_start_0`, 
      e.author_id AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'User' AS `__cg_end_label`, 
      concat('AUTHORED:User:Post:', to_json(named_struct('_1', CAST(e.author_id AS STRING), '_2', CAST(e.post_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'AUTHORED' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.author_id AS `__cg_rid_0`, 
      e.post_id AS `__cg_rid_1`, 
      e.author_id AS `__cg_from_0`, 
      e.post_id AS `__cg_to_0`, 
      e.author_id AS `__cg_start_0`, 
      e.post_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('AUTHORED:User:Post:', to_json(named_struct('_1', CAST(e.author_id AS STRING), '_2', CAST(e.post_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM test_integration.posts_test AS e
)
SELECT 
      to_variant_object(map('elementId', CAST(concat(v1.__cg_type, ':', CAST(v1.__cg_from_0 AS STRING), '->', CAST(v1.__cg_to_0 AS STRING), '-') AS VARIANT), 'startNodeElementId', CAST(concat(v1.__cg_from_label, ':', CAST(v1.__cg_from_0 AS STRING), '-') AS VARIANT), 'endNodeElementId', CAST(concat(v1.__cg_to_label, ':', CAST(v1.__cg_to_0 AS STRING), '-') AS VARIANT), 'type', CAST(v1.__cg_type AS VARIANT), 'properties', CAST(to_variant_object(map_filter(CAST(map() AS MAP<STRING, VARIANT>), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))) AS `r`
FROM v0_labels AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_label = v0.__cg_label AND v1.__cg_start_0 = v0.__cg_id_0
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
LIMIT 25
-- column r: Graph(Relationship) [("r", "r")]