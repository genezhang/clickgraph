WITH RECURSIVE with_c_z_cte_0 AS (SELECT 
      t0.to_id AS `p1_c_user_id`, 
      z.user_id AS `p1_z_user_id`
FROM brahmand.users_bench AS z
INNER JOIN brahmand.interactions AS t0 ON t0.from_id = z.user_id AND t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User'
), 
vlp_z_b AS (
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
    FROM vlp_z_b vp
    JOIN brahmand.interactions AS rel ON vp.end_id = rel.from_id
    JOIN brahmand.users_bench AS end_node ON rel.to_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp))
      AND rel.interaction_type = 'FOLLOWS' AND rel.from_type = 'User' AND rel.to_type = 'User'
)
SELECT 
      count(*) AS `k`
FROM vlp_z_b AS t
JOIN brahmand.users_bench AS a ON 1 = 1
INNER JOIN brahmand.interactions AS t1 ON t1.from_id = a.user_id AND t1.interaction_type = 'FOLLOWS' AND t1.from_type = 'User' AND t1.to_type = 'User'
INNER JOIN with_c_z_cte_0 AS c_z ON c_z.p1_c_user_id = t1.to_id AND string(t.start_id) = string(c_z.p1_z_user_id)
INNER JOIN brahmand.interactions AS t2 ON t2.from_id = t.start_id AND t2.to_id = c_z.p1_c_user_id AND t2.interaction_type = 'FOLLOWS' AND t2.from_type = 'User' AND t2.to_type = 'User'
WHERE ((NOT array_contains(t.path_edges, struct(t2.from_id, t2.to_id, t2.interaction_type, t2.timestamp)) AND NOT array_contains(t.path_edges, struct(t1.from_id, t1.to_id, t1.interaction_type, t1.timestamp))) AND (t2.from_id <> t1.from_id OR t2.to_id <> t1.to_id OR t2.interaction_type <> t1.interaction_type OR t2.timestamp <> t1.timestamp))
