WITH RECURSIVE vlp_a_b_inner AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [tuple(rel.follower_id, rel.followed_id)] as path_edges,
        end_node.name as end_name
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.follower_id, rel.followed_id)]) as path_edges,
        end_node.name as end_name
    FROM vlp_a_b_inner vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.follower_id, rel.followed_id))
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE (end_name = 'Bob') AND hop_count >= 2
)
SELECT 
      a.name AS "a.name", 
      vt0.end_name AS "b.name"
FROM test_integration.users AS a
LEFT JOIN vlp_a_b AS vt0 ON a.user_id = vt0.start_id
