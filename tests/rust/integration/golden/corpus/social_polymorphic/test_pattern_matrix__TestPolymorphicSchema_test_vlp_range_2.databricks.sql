WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        start_node.full_name as start_name,
        end_node.full_name as end_name
    FROM brahmand.users_bench AS start_node
    JOIN brahmand.interactions AS rel ON start_node.user_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE rel.interaction_type = 'LIKES' AND rel.from_type = 'User' AND rel.to_type = 'User'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        vp.start_name as start_name,
        end_node.full_name as end_name
    FROM vlp_a_b vp
    JOIN brahmand.interactions AS rel ON vp.end_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_nodes, end_node.user_id)
      AND rel.interaction_type = 'LIKES' AND rel.from_type = 'User' AND rel.to_type = 'User'
)
SELECT 
      t.start_name AS `a.name`, 
      t.end_name AS `b.name`
FROM vlp_a_b AS t
LIMIT 10