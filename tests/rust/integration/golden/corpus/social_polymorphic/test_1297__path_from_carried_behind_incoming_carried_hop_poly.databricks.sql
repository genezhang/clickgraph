WITH RECURSIVE with_w1 AS (
SELECT 
      v2.user_id AS `v3__user_id`, 
      v0.user_id AS `v4__user_id`
FROM brahmand.users_bench AS v0
JOIN brahmand.interactions AS v1 ON v1.from_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.to_id = v2.user_id
WHERE ((((v1.`interaction_type` = 'FOLLOWS') AND (v1.`from_type` = 'User')) AND (v1.`to_type` = 'User')))
), 
vlp_v8_nodes AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`, 
      concat('Post', ':', CAST(e.post_id AS STRING)) AS `__cg_key`
FROM brahmand.posts_bench AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      concat('User', ':', CAST(e.user_id AS STRING)) AS `__cg_key`
FROM brahmand.users_bench AS e
), 
v8_rels AS (
SELECT 
      'FOLLOWS' AS `__cg_type`, 
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
      concat('FOLLOWS:Post:Post:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING), '_3', CAST(e.interaction_type AS STRING), '_4', CAST(e.timestamp AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM brahmand.interactions AS e
WHERE ((((e.`interaction_type` = 'FOLLOWS') AND (e.`from_type` = 'Post')) AND (e.`to_type` = 'Post')))

UNION ALL
SELECT 
      'FOLLOWS' AS `__cg_type`, 
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
      concat('FOLLOWS:Post:User:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING), '_3', CAST(e.interaction_type AS STRING), '_4', CAST(e.timestamp AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM brahmand.interactions AS e
WHERE ((((e.`interaction_type` = 'FOLLOWS') AND (e.`from_type` = 'Post')) AND (e.`to_type` = 'User')))

UNION ALL
SELECT 
      'FOLLOWS' AS `__cg_type`, 
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
      concat('FOLLOWS:User:Post:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING), '_3', CAST(e.interaction_type AS STRING), '_4', CAST(e.timestamp AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM brahmand.interactions AS e
WHERE ((((e.`interaction_type` = 'FOLLOWS') AND (e.`from_type` = 'User')) AND (e.`to_type` = 'Post')))

UNION ALL
SELECT 
      'FOLLOWS' AS `__cg_type`, 
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
      concat('FOLLOWS:User:User:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING), '_3', CAST(e.interaction_type AS STRING), '_4', CAST(e.timestamp AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM brahmand.interactions AS e
WHERE ((((e.`interaction_type` = 'FOLLOWS') AND (e.`from_type` = 'User')) AND (e.`to_type` = 'User')))
), 
vlp_v8_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS INT) AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_edges, array(start_node.__cg_key) AS path_nodes
    FROM vlp_v8_nodes AS start_node
    WHERE (start_node.__cg_label = 'User') AND ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      'User' AS `label`, 
      w1.v4__user_id AS `id`
FROM with_w1 AS w1
CROSS JOIN brahmand.users_bench AS v5
JOIN brahmand.interactions AS v6 ON v6.from_id = v5.user_id AND v6.to_id = w1.v3__user_id
JOIN brahmand.interactions AS v7 ON v7.from_id = w1.v4__user_id AND v7.to_id = w1.v3__user_id))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes
    FROM vlp_v8_trails AS vp
    JOIN v8_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v8_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v8_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v8_trails
    WHERE hop_count >= 1
)
SELECT 
      count(*) AS `k`
FROM vlp_v8_path AS v8
JOIN with_w1 AS w1 ON v8.start_id = w1.v4__user_id
JOIN brahmand.interactions AS v6 ON v6.to_id = w1.v3__user_id
JOIN brahmand.users_bench AS v5 ON v6.from_id = v5.user_id
JOIN brahmand.interactions AS v7 ON v7.from_id = w1.v4__user_id AND v7.to_id = w1.v3__user_id
JOIN brahmand.users_bench AS v9 ON v8.end_id = v9.user_id
WHERE (((((v6.`interaction_type` = 'FOLLOWS') AND (v6.`from_type` = 'User')) AND (v6.`to_type` = 'User'))) AND ((((v7.`interaction_type` = 'FOLLOWS') AND (v7.`from_type` = 'User')) AND (v7.`to_type` = 'User'))) AND v8.end_label = 'User' AND (v6.from_id <> v7.from_id OR v6.to_id <> v7.to_id OR v6.interaction_type <> v7.interaction_type OR v6.timestamp <> v7.timestamp) AND NOT array_contains(v8.path_edges, concat('FOLLOWS:User:User:', to_json(named_struct('_1', CAST(v6.from_id AS STRING), '_2', CAST(v6.to_id AS STRING), '_3', CAST(v6.interaction_type AS STRING), '_4', CAST(v6.timestamp AS STRING)), map('ignoreNullFields', 'false')))) AND NOT array_contains(v8.path_edges, concat('FOLLOWS:User:User:', to_json(named_struct('_1', CAST(v7.from_id AS STRING), '_2', CAST(v7.to_id AS STRING), '_3', CAST(v7.interaction_type AS STRING), '_4', CAST(v7.timestamp AS STRING)), map('ignoreNullFields', 'false')))))

-- column k: Value [("k", "k")]