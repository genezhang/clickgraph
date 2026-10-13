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
      concat('LIKES:Post:Post:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key", 
      concat('Post', ':', toString(e.from_id)) AS "__cg_start_key", 
      concat('Post', ':', toString(e.to_id)) AS "__cg_end_key"
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
      concat('LIKES:Post:User:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key", 
      concat('Post', ':', toString(e.from_id)) AS "__cg_start_key", 
      concat('User', ':', toString(e.to_id)) AS "__cg_end_key"
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
      concat('LIKES:User:Post:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key", 
      concat('User', ':', toString(e.from_id)) AS "__cg_start_key", 
      concat('Post', ':', toString(e.to_id)) AS "__cg_end_key"
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
      concat('LIKES:User:User:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key", 
      concat('User', ':', toString(e.from_id)) AS "__cg_start_key", 
      concat('User', ':', toString(e.to_id)) AS "__cg_end_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'LIKES') AND (e."from_type" = 'User')) AND (e."to_type" = 'User')))
), 
vlp_v1_bfs AS (
    SELECT DISTINCT start_node.__cg_key AS start_id, start_node.__cg_key AS node, CAST(0 AS UInt32) AS depth, CAST(1 AS UInt8) AS new
    FROM vlp_v1_nodes AS start_node
    WHERE start_node.__cg_key IS NOT NULL AND (start_node.__cg_label = 'User')
    UNION ALL
    SELECT DISTINCT f.start_id AS start_id, end_node.__cg_key AS node, CAST(f.depth + 1 AS UInt32) AS depth, CAST(1 AS UInt8) AS new
    FROM vlp_v1_bfs AS f
    JOIN v1_rels AS rel ON rel.__cg_start_key = f.node
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_key = rel.__cg_end_key
    WHERE f.new = 1
      AND f.depth < 5
      AND (f.start_id, end_node.__cg_key) NOT IN (SELECT start_id, node FROM vlp_v1_bfs)
      AND f.start_id NOT IN (SELECT start_id FROM vlp_v1_bfs GROUP BY start_id HAVING countIf(node IN (SELECT end_node.__cg_key FROM vlp_v1_nodes AS end_node WHERE (end_node.__cg_label = 'User'))) >= (SELECT count(DISTINCT end_node.__cg_key) FROM (SELECT end_node.__cg_key FROM vlp_v1_nodes AS end_node WHERE (end_node.__cg_label = 'User')) AS end_node))
    UNION ALL
    SELECT start_id, node, depth, CAST(0 AS UInt8) AS new
    FROM vlp_v1_bfs
    WHERE start_id IN (SELECT start_id FROM vlp_v1_bfs WHERE new = 1)
), 
vlp_v1_path AS (
    SELECT start_id, node AS end_id, depth AS hop_count FROM vlp_v1_bfs
    WHERE new = 1 AND depth >= 1
      AND node IN (SELECT end_node.__cg_key FROM vlp_v1_nodes AS end_node WHERE (end_node.__cg_label = 'User'))
), 
vlp_v1_ends AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, p.hop_count AS hop_count
    FROM vlp_v1_path AS p
    JOIN vlp_v1_nodes AS start_node ON start_node.__cg_key = p.start_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_key = p.end_id
)
SELECT 
      v1.hop_count AS "length(p)"
FROM vlp_v1_ends AS v1
JOIN brahmand.users_bench AS v0 ON v1.start_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.end_id = v2.user_id
WHERE (v1.end_label = 'User' AND v0.user_id <> v2.user_id)
LIMIT 5
-- column length(p): Value [("length(p)", "length(p)")]