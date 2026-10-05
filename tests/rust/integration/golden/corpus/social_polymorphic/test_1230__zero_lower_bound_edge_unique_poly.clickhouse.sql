WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id] as path_nodes,
        (
            SELECT arraySlice([tuple(__seed_edge.from_id, __seed_edge.to_id, __seed_edge.interaction_type, __seed_edge.timestamp)], 1, 0)
            FROM brahmand.interactions AS __seed_edge
            LIMIT 1
        ) as path_edges,
        start_node.email_address as start_email,
        start_node.full_name as start_name,
        start_node.email_address as end_email,
        start_node.full_name as end_name
    FROM brahmand.users_bench AS start_node
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp)]) as path_edges,
        vp.start_email as start_email,
        vp.start_name as start_name,
        end_node.email_address as end_email,
        end_node.full_name as end_name
    FROM vlp_a_b vp
    JOIN brahmand.interactions AS rel ON vp.end_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp))
      AND rel.interaction_type = 'FOLLOWS' AND rel.from_type = 'User' AND rel.to_type = 'User'
)
SELECT 
      count(*) AS "count(*)"
FROM vlp_a_b AS t
