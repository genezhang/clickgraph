WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id] as path_nodes,
        (
            SELECT arraySlice([tuple(__seed_edge.follower_id, __seed_edge.followed_id)], 1, 0)
            FROM test_integration.follows AS __seed_edge
            LIMIT 1
        ) as path_edges
    FROM test_integration.users AS start_node
    WHERE (start_node.name = 'Alice')
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.follower_id, rel.followed_id)]) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2147483647
      AND NOT has(vp.path_edges, tuple(rel.follower_id, rel.followed_id))
)
SELECT DISTINCT 
      v2.name AS "b.name"
FROM vlp_v1_path AS v1
JOIN test_integration.users AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.end_id = v2.user_id
WHERE v0.name = 'Alice'
ORDER BY v2.name ASC
