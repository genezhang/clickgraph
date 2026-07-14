WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(struct(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp)) as path_edges
    FROM brahmand.users_bench AS start_node
    JOIN brahmand.interactions AS rel ON start_node.user_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE rel.interaction_type = 'FOLLOWS' AND rel.from_type = 'User' AND rel.to_type = 'User'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp))) as path_edges
    FROM vlp_a_b vp
    JOIN brahmand.interactions AS rel ON vp.end_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp))
      AND rel.interaction_type = 'FOLLOWS' AND rel.from_type = 'User' AND rel.to_type = 'User'
)
SELECT 
      t.end_id AS `b.user_id`
FROM vlp_a_b AS t
