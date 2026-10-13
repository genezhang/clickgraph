WITH RECURSIVE vlp_v1_nodes AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`, 
      concat('Post', ':', CAST(e.post_id AS STRING)) AS `__cg_key`, 
      to_variant_object(map('elementId', CAST(concat('Post:', CAST(e.post_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('Post') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('content', CAST(e.content AS VARIANT), 'created', CAST(e.created_at AS VARIANT), 'post_id', CAST(e.post_id AS VARIANT), 'title', CAST(e.content AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))) AS `__cg_value`
FROM brahmand.posts_bench AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      concat('User', ':', CAST(e.user_id AS STRING)) AS `__cg_key`, 
      to_variant_object(map('elementId', CAST(concat('User:', CAST(e.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('email', CAST(e.email_address AS VARIANT), 'name', CAST(e.full_name AS VARIANT), 'user_id', CAST(e.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))) AS `__cg_value`
FROM brahmand.users_bench AS e
), 
v1_rels AS (
SELECT 
      'LIKES' AS `__cg_type`, 
      'Post' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.from_id AS `__cg_rid_0`, 
      e.to_id AS `__cg_rid_1`, 
      e.interaction_type AS `__cg_rid_2`, 
      e.timestamp AS `__cg_rid_3`, 
      e.from_id AS `__cg_from_0`, 
      e.to_id AS `__cg_to_0`, 
      e.from_id AS `__cg_start_0`, 
      e.to_id AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('LIKES:Post:Post:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING), '_3', CAST(e.interaction_type AS STRING), '_4', CAST(e.timestamp AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM brahmand.interactions AS e
WHERE ((((e.`interaction_type` = 'LIKES') AND (e.`from_type` = 'Post')) AND (e.`to_type` = 'Post')))

UNION ALL
SELECT 
      'LIKES' AS `__cg_type`, 
      'Post' AS `__cg_from_label`, 
      'User' AS `__cg_to_label`, 
      e.from_id AS `__cg_rid_0`, 
      e.to_id AS `__cg_rid_1`, 
      e.interaction_type AS `__cg_rid_2`, 
      e.timestamp AS `__cg_rid_3`, 
      e.from_id AS `__cg_from_0`, 
      e.to_id AS `__cg_to_0`, 
      e.from_id AS `__cg_start_0`, 
      e.to_id AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'User' AS `__cg_end_label`, 
      concat('LIKES:Post:User:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING), '_3', CAST(e.interaction_type AS STRING), '_4', CAST(e.timestamp AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM brahmand.interactions AS e
WHERE ((((e.`interaction_type` = 'LIKES') AND (e.`from_type` = 'Post')) AND (e.`to_type` = 'User')))

UNION ALL
SELECT 
      'LIKES' AS `__cg_type`, 
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
      concat('LIKES:User:Post:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING), '_3', CAST(e.interaction_type AS STRING), '_4', CAST(e.timestamp AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM brahmand.interactions AS e
WHERE ((((e.`interaction_type` = 'LIKES') AND (e.`from_type` = 'User')) AND (e.`to_type` = 'Post')))

UNION ALL
SELECT 
      'LIKES' AS `__cg_type`, 
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
      concat('LIKES:User:User:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING), '_3', CAST(e.interaction_type AS STRING), '_4', CAST(e.timestamp AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM brahmand.interactions AS e
WHERE ((((e.`interaction_type` = 'LIKES') AND (e.`from_type` = 'User')) AND (e.`to_type` = 'User')))
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS INT) AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_edges, array(start_node.__cg_key) AS path_nodes, array(start_node.__cg_value) AS path_node_values
    FROM vlp_v1_nodes AS start_node
    WHERE (start_node.__cg_label = 'User')
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes, concat(vp.path_node_values, array(end_node.__cg_value)) AS path_node_values
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes, path_node_values
    FROM vlp_v1_trails
    WHERE hop_count >= 1
)
SELECT 
      v1.hop_count AS `length(p)`, 
      concat(array(to_variant_object(map('elementId', CAST(concat('User:', CAST(v0.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('email', CAST(v0.email_address AS VARIANT), 'name', CAST(v0.full_name AS VARIANT), 'user_id', CAST(v0.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT)))), filter(v1.path_node_values, (__x, __i) -> __i > 0)) AS `nodes(p)`
FROM vlp_v1_path AS v1
JOIN brahmand.users_bench AS v0 ON v1.start_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.end_id = v2.user_id
WHERE v1.end_label = 'User'
LIMIT 5