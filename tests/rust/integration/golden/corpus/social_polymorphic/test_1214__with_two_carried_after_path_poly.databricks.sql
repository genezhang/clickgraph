WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(struct(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp)) as path_edges,
        end_node.email_address as end_email,
        end_node.full_name as end_name
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
        concat(vp.path_edges, array(struct(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp))) as path_edges,
        end_node.email_address as end_email,
        end_node.full_name as end_name
    FROM vlp_a_b vp
    JOIN brahmand.interactions AS rel ON vp.end_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp))
      AND rel.interaction_type = 'FOLLOWS' AND rel.from_type = 'User' AND rel.to_type = 'User'
), 
with_b_c_cte_0 AS (SELECT 
      end_id AS `p1_b_user_id`
FROM vlp_a_b AS t
JOIN brahmand.users_bench AS c ON 1 = 1
INNER JOIN brahmand.interactions AS t0 ON t0.from_id = c.user_id AND t0.to_id = t.start_id AND t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User'
WHERE NOT array_contains(t.path_edges, struct(t0.from_id, t0.to_id, t0.interaction_type, t0.timestamp))
)
SELECT 
      count(*) AS `count(*)`
FROM brahmand.interactions AS t1
INNER JOIN with_b_c_cte_0 AS b ON t1.from_id = b.p1_b_user_id
WHERE t1.interaction_type = 'FOLLOWS' AND t1.from_type = 'User' AND t1.to_type = 'User'
