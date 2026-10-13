WITH RECURSIVE with_w1 AS (
SELECT 
      v2.user_id AS `v3__user_id`, 
      v2.user_id AS `p2_v3_user_id`
FROM brahmand.users_bench AS v0
JOIN brahmand.interactions AS v1 ON v1.from_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.to_id = v2.user_id
WHERE ((((v1.`interaction_type` = 'FOLLOWS') AND (v1.`from_type` = 'User')) AND (v1.`to_type` = 'User')))
), 
vlp_v6_nodes AS (
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
v6_rels AS (
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
vlp_v6_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS INT) AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_edges, array(start_node.__cg_key) AS path_nodes
    FROM vlp_v6_nodes AS start_node
    WHERE (start_node.__cg_label = 'User') AND ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      'User' AS `label`, 
      v5.user_id AS `id`
FROM with_w1 AS w1
JOIN brahmand.interactions AS v4 ON v4.from_id = w1.v3__user_id
JOIN brahmand.users_bench AS v5 ON v4.to_id = v5.user_id))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes
    FROM vlp_v6_trails AS vp
    JOIN v6_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v6_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v6_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v6_trails
    WHERE hop_count >= 1
)
SELECT 
      w1.p2_v3_user_id AS `c.user_id`, 
      v5.user_id AS `a.user_id`, 
      v7.user_id AS `b.user_id`
FROM vlp_v6_path AS v6
JOIN brahmand.users_bench AS v5 ON v6.start_id = v5.user_id
JOIN brahmand.interactions AS v4 ON v4.to_id = v5.user_id
JOIN with_w1 AS w1 ON v4.from_id = w1.v3__user_id
JOIN brahmand.users_bench AS v7 ON v6.end_id = v7.user_id
WHERE (((((v4.`interaction_type` = 'FOLLOWS') AND (v4.`from_type` = 'User')) AND (v4.`to_type` = 'User'))) AND v6.end_label = 'User' AND NOT array_contains(v6.path_edges, concat('FOLLOWS:User:User:', to_json(named_struct('_1', CAST(v4.from_id AS STRING), '_2', CAST(v4.to_id AS STRING), '_3', CAST(v4.interaction_type AS STRING), '_4', CAST(v4.timestamp AS STRING)), map('ignoreNullFields', 'false')))))

-- column c.user_id: Value [("c.user_id", "c.user_id")]
-- column a.user_id: Value [("a.user_id", "a.user_id")]
-- column b.user_id: Value [("b.user_id", "b.user_id")]