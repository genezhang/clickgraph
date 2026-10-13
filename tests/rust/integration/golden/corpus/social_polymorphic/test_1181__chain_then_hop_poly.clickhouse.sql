WITH RECURSIVE vlp_v1_nodes AS (
SELECT 
      'Post' AS "__cg_label", 
      e.post_id AS "__cg_id_0", 
      concat('Post', ':', toString(e.post_id)) AS "__cg_key"
FROM brahmand.posts_bench AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      concat('User', ':', toString(e.user_id)) AS "__cg_key"
FROM brahmand.users_bench AS e
), 
v1_rels AS (
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('FOLLOWS:Post:Post:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'Post')) AND (e."to_type" = 'Post')))

UNION ALL
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'User' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'User' AS "__cg_end_label", 
      concat('FOLLOWS:Post:User:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'Post')) AND (e."to_type" = 'User')))

UNION ALL
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('FOLLOWS:User:Post:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'User')) AND (e."to_type" = 'Post')))

UNION ALL
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'User' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'User' AS "__cg_end_label", 
      concat('FOLLOWS:User:User:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'User')) AND (e."to_type" = 'User')))
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS UInt32) AS hop_count, CAST([], 'Array(String)') AS path_edges, [start_node.__cg_key] AS path_nodes
    FROM vlp_v1_nodes AS start_node
    WHERE (start_node.__cg_label = 'User')
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS UInt32) AS hop_count, arrayConcat(vp.path_edges, [rel.__cg_key]) AS path_edges, arrayConcat(vp.path_nodes, [end_node.__cg_key]) AS path_nodes
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v1_trails
    WHERE hop_count >= 1
), 
vlp_v3_nodes AS (
SELECT 
      'Post' AS "__cg_label", 
      e.post_id AS "__cg_id_0", 
      concat('Post', ':', toString(e.post_id)) AS "__cg_key"
FROM brahmand.posts_bench AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      concat('User', ':', toString(e.user_id)) AS "__cg_key"
FROM brahmand.users_bench AS e
), 
v3_rels AS (
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('FOLLOWS:Post:Post:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'Post')) AND (e."to_type" = 'Post')))

UNION ALL
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'User' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'User' AS "__cg_end_label", 
      concat('FOLLOWS:Post:User:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'Post')) AND (e."to_type" = 'User')))

UNION ALL
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('FOLLOWS:User:Post:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'User')) AND (e."to_type" = 'Post')))

UNION ALL
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'User' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'User' AS "__cg_end_label", 
      concat('FOLLOWS:User:User:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'User')) AND (e."to_type" = 'User')))
), 
vlp_v3_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS UInt32) AS hop_count, CAST([], 'Array(String)') AS path_edges, [start_node.__cg_key] AS path_nodes
    FROM vlp_v3_nodes AS start_node
    WHERE (start_node.__cg_label = 'User') AND ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      'User' AS "label", 
      v2.user_id AS "id"
FROM brahmand.users_bench AS v0
JOIN vlp_v1_path AS v1 ON v1.start_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.end_id = v2.user_id
WHERE v1.end_label = 'User'))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS UInt32) AS hop_count, arrayConcat(vp.path_edges, [rel.__cg_key]) AS path_edges, arrayConcat(vp.path_nodes, [end_node.__cg_key]) AS path_nodes
    FROM vlp_v3_trails AS vp
    JOIN v3_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v3_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.__cg_key)
), 
vlp_v3_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v3_trails
    WHERE hop_count >= 1
)
SELECT 
      count(*) AS "count(*)"
FROM vlp_v1_path AS v1
JOIN brahmand.users_bench AS v0 ON v1.start_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.end_id = v2.user_id
JOIN vlp_v3_path AS v3 ON v3.start_id = v2.user_id
JOIN brahmand.users_bench AS v4 ON v3.end_id = v4.user_id
JOIN brahmand.interactions AS v5 ON v5.from_id = v4.user_id
JOIN brahmand.users_bench AS v6 ON v5.to_id = v6.user_id
WHERE (v1.end_label = 'User' AND v3.end_label = 'User' AND ((((v5."interaction_type" = 'FOLLOWS') AND (v5."from_type" = 'User')) AND (v5."to_type" = 'User'))) AND NOT hasAny(v1.path_edges, v3.path_edges) AND NOT has(v1.path_edges, concat('FOLLOWS:User:User:', toString(tuple(v5.from_id, v5.to_id, v5.interaction_type, v5.timestamp)))) AND NOT has(v3.path_edges, concat('FOLLOWS:User:User:', toString(tuple(v5.from_id, v5.to_id, v5.interaction_type, v5.timestamp)))))
