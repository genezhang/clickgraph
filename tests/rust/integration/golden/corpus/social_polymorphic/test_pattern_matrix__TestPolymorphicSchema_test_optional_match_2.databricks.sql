WITH v2_labels AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`
FROM brahmand.posts_bench AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`
FROM brahmand.users_bench AS e
), 
v1_rels AS (
SELECT 
      'AUTHORED' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.from_id AS `__cg_rid_0`, 
      e.to_id AS `__cg_rid_1`, 
      e.interaction_type AS `__cg_rid_2`, 
      e.timestamp AS `__cg_rid_3`, 
      e.from_id AS `__cg_from_0`, 
      e.to_id AS `__cg_to_0`, 
      e.from_id AS `__cg_start_0`, 
      e.to_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('AUTHORED:User:Post:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING), '_3', CAST(e.interaction_type AS STRING), '_4', CAST(e.timestamp AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM brahmand.interactions AS e
WHERE ((((e.`interaction_type` = 'AUTHORED') AND (e.`from_type` = 'User')) AND (e.`to_type` = 'Post')))

UNION ALL
SELECT 
      'AUTHORED' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'User' AS `__cg_to_label`, 
      e.from_id AS `__cg_rid_0`, 
      e.to_id AS `__cg_rid_1`, 
      e.interaction_type AS `__cg_rid_2`, 
      e.timestamp AS `__cg_rid_3`, 
      e.from_id AS `__cg_from_0`, 
      e.to_id AS `__cg_to_0`, 
      e.from_id AS `__cg_start_0`, 
      e.to_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'User' AS `__cg_end_label`, 
      concat('AUTHORED:User:User:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING), '_3', CAST(e.interaction_type AS STRING), '_4', CAST(e.timestamp AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM brahmand.interactions AS e
WHERE ((((e.`interaction_type` = 'AUTHORED') AND (e.`from_type` = 'User')) AND (e.`to_type` = 'User')))
), 
optional_o1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v1.__cg_type AS `v1____cg_type`, 
      v1.__cg_from_label AS `v1____cg_from_label`, 
      v1.__cg_to_label AS `v1____cg_to_label`, 
      v1.__cg_rid_0 AS `v1____cg_rid_0`, 
      v1.__cg_rid_1 AS `v1____cg_rid_1`, 
      v1.__cg_rid_2 AS `v1____cg_rid_2`, 
      v1.__cg_rid_3 AS `v1____cg_rid_3`, 
      v1.__cg_from_0 AS `v1____cg_from_0`, 
      v1.__cg_to_0 AS `v1____cg_to_0`, 
      v1.__cg_key AS `v1____cg_key`, 
      v2.__cg_label AS `v2____cg_label`, 
      v2.__cg_id_0 AS `v2____cg_id_0`
FROM brahmand.users_bench AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
)
SELECT 
      v0.email_address AS `a.email`, 
      count(o1.v1____cg_type) AS `rel_count`
FROM brahmand.users_bench AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
GROUP BY v0.email_address
