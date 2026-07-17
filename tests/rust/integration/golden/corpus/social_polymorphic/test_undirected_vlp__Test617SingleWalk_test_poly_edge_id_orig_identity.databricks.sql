WITH RECURSIVE undir_edges_a_b_brahmand_interactions AS (
    SELECT e.from_id, e.to_id, e.from_type, e.interaction_type, e.interaction_weight, e.timestamp, e.to_type, e.from_id AS __cg_orig_from, e.to_id AS __cg_orig_to FROM brahmand.interactions AS e
    UNION ALL
    SELECT e.to_id AS from_id, e.from_id AS to_id, e.from_type, e.interaction_type, e.interaction_weight, e.timestamp, e.to_type, e.from_id AS __cg_orig_from, e.to_id AS __cg_orig_to FROM brahmand.interactions AS e
),
vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(struct(rel.__cg_orig_from, rel.__cg_orig_to, rel.interaction_type, rel.timestamp)) as path_edges
    FROM brahmand.users_bench AS start_node
    JOIN undir_edges_a_b_brahmand_interactions AS rel ON start_node.user_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE rel.interaction_type = 'FOLLOWS' AND rel.from_type = 'User' AND rel.to_type = 'User'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.__cg_orig_from, rel.__cg_orig_to, rel.interaction_type, rel.timestamp))) as path_edges
    FROM vlp_a_b vp
    JOIN undir_edges_a_b_brahmand_interactions AS rel ON vp.end_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.__cg_orig_from, rel.__cg_orig_to, rel.interaction_type, rel.timestamp))
      AND rel.interaction_type = 'FOLLOWS' AND rel.from_type = 'User' AND rel.to_type = 'User'
)
SELECT 
      count(*) AS `count(*)`
FROM vlp_a_b AS t
