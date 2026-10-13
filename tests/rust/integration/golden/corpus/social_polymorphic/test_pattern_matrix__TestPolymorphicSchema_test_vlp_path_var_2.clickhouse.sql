WITH RECURSIVE vlp_v1_nodes AS (
SELECT 
      'Post' AS "__cg_label", 
      e.post_id AS "__cg_id_0", 
      concat('Post', ':', toString(e.post_id)) AS "__cg_key", 
      map('elementId', CAST(concat('Post:', toString(e.post_id), '-'), 'Dynamic'), 'labels', CAST(['Post'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('content', CAST(e.content, 'Dynamic'), 'created', CAST(e.created_at, 'Dynamic'), 'post_id', CAST(e.post_id, 'Dynamic'), 'title', CAST(e.content, 'Dynamic'))), 'Dynamic')) AS "__cg_value"
FROM brahmand.posts_bench AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      concat('User', ':', toString(e.user_id)) AS "__cg_key", 
      map('elementId', CAST(concat('User:', toString(e.user_id), '-'), 'Dynamic'), 'labels', CAST(['User'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('email', CAST(e.email_address, 'Dynamic'), 'name', CAST(e.full_name, 'Dynamic'), 'user_id', CAST(e.user_id, 'Dynamic'))), 'Dynamic')) AS "__cg_value"
FROM brahmand.users_bench AS e
), 
v1_rels AS (
SELECT 
      'LIKES' AS "__cg_type", 
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
      concat('LIKES:Post:Post:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'LIKES') AND (e."from_type" = 'Post')) AND (e."to_type" = 'Post')))

UNION ALL
SELECT 
      'LIKES' AS "__cg_type", 
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
      concat('LIKES:Post:User:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'LIKES') AND (e."from_type" = 'Post')) AND (e."to_type" = 'User')))

UNION ALL
SELECT 
      'LIKES' AS "__cg_type", 
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
      concat('LIKES:User:Post:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'LIKES') AND (e."from_type" = 'User')) AND (e."to_type" = 'Post')))

UNION ALL
SELECT 
      'LIKES' AS "__cg_type", 
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
      concat('LIKES:User:User:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'LIKES') AND (e."from_type" = 'User')) AND (e."to_type" = 'User')))
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS UInt32) AS hop_count, CAST([], 'Array(String)') AS path_edges, [start_node.__cg_key] AS path_nodes, [start_node.__cg_value] AS path_node_values
    FROM vlp_v1_nodes AS start_node
    WHERE (start_node.__cg_label = 'User')
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS UInt32) AS hop_count, arrayConcat(vp.path_edges, [rel.__cg_key]) AS path_edges, arrayConcat(vp.path_nodes, [end_node.__cg_key]) AS path_nodes, arrayConcat(vp.path_node_values, [end_node.__cg_value]) AS path_node_values
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes, path_node_values
    FROM vlp_v1_trails
    WHERE hop_count >= 1
)
SELECT 
      v1.hop_count AS "length(p)", 
      arrayConcat([map('elementId', CAST(concat('User:', toString(v0.user_id), '-'), 'Dynamic'), 'labels', CAST(['User'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('email', CAST(v0.email_address, 'Dynamic'), 'name', CAST(v0.full_name, 'Dynamic'), 'user_id', CAST(v0.user_id, 'Dynamic'))), 'Dynamic'))], arraySlice(v1.path_node_values, 2)) AS "nodes(p)"
FROM vlp_v1_path AS v1
JOIN brahmand.users_bench AS v0 ON v1.start_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.end_id = v2.user_id
WHERE v1.end_label = 'User'
LIMIT 5