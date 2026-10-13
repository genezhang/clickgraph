WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [tuple(rel.follower_id, rel.followed_id)] as path_edges
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
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
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.follower_id, rel.followed_id))
)
SELECT DISTINCT 
      v1.hop_count AS "path_length", 
      v1.hop_count + 1 AS "node_count", 
      v1.hop_count AS "rel_count"
FROM vlp_v1_path AS v1
JOIN test_integration.users AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.end_id = v2.user_id
WHERE v0.name = 'Alice'
ORDER BY v1.hop_count ASC

-- column path_length: Value [("path_length", "path_length")]
-- column node_count: Value [("node_count", "node_count")]
-- column rel_count: Value [("rel_count", "rel_count")]