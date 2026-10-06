WITH RECURSIVE with_c_cte_0 AS (SELECT 
      t0.to_id AS `p1_c_user_id`
FROM brahmand.users_bench AS z
INNER JOIN brahmand.interactions AS t0 ON t0.from_id = z.user_id AND t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User'
), 
vlp_a_b_inner AS (
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
    FROM vlp_a_b_inner vp
    JOIN brahmand.interactions AS rel ON vp.end_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp))
      AND rel.interaction_type = 'FOLLOWS' AND rel.from_type = 'User' AND rel.to_type = 'User'
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE hop_count >= 2
)
SELECT 
      c.p1_c_user_id AS `c.user_id`, 
      t.end_id AS `b.user_id`
FROM vlp_a_b AS t
INNER JOIN with_c_cte_0 AS c ON 1 = 1
INNER JOIN brahmand.interactions AS t1 ON t1.from_id = c.p1_c_user_id AND t1.to_id = t.start_id AND t1.interaction_type = 'FOLLOWS' AND t1.from_type = 'User' AND t1.to_type = 'User'
WHERE NOT array_contains(t.path_edges, struct(t1.from_id, t1.to_id, t1.interaction_type, t1.timestamp))
