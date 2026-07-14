WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [tuple(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp)] as path_edges
    FROM brahmand.users_bench AS start_node
    JOIN brahmand.interactions AS rel ON start_node.user_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE rel.interaction_type = 'FOLLOWS' AND rel.from_type = 'User' AND rel.to_type = 'User'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp)]) as path_edges
    FROM vlp_a_b vp
    JOIN brahmand.interactions AS rel ON vp.end_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp))
      AND rel.interaction_type = 'FOLLOWS' AND rel.from_type = 'User' AND rel.to_type = 'User'
)
SELECT 
      t.end_id AS "b.user_id"
FROM vlp_a_b AS t
