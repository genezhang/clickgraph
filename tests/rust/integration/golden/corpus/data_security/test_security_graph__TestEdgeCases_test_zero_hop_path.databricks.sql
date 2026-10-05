WITH RECURSIVE vlp_u_target AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id) as path_nodes,
        (
            SELECT slice(array(struct(__seed_edge.member_id, __seed_edge.group_id)), 1, 0)
            FROM data_security.ds_memberships AS __seed_edge
            LIMIT 1
        ) as path_edges,
        start_node.name as start_name,
        '' as end_description,
        start_node.name as end_name
    FROM data_security.ds_users AS start_node
    WHERE start_node.name = 'Alice'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.group_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.group_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.member_id, rel.group_id))) as path_edges,
        vp.start_name as start_name,
        end_node.description as end_description,
        end_node.name as end_name
    FROM vlp_u_target vp
    JOIN data_security.ds_memberships AS rel ON vp.end_id = rel.member_id
    JOIN data_security.ds_groups AS end_node ON rel.group_id = end_node.group_id
    WHERE vp.hop_count < 1
      AND NOT array_contains(vp.path_edges, struct(rel.member_id, rel.group_id))
      AND rel.member_type = 'Group'
)
SELECT 
      t.end_description AS `target.description`, 
      t.end_id AS `target.group_id`, 
      t.end_name AS `target.name`
FROM vlp_u_target AS t
