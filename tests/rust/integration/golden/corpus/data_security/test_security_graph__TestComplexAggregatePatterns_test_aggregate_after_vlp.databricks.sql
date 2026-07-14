WITH RECURSIVE vlp_u_g AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.group_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.group_id) as path_nodes,
        array(struct(rel.member_id, rel.group_id)) as path_edges,
        start_node.name as start_name
    FROM data_security.ds_users AS start_node
    JOIN data_security.ds_memberships AS rel ON start_node.user_id = rel.member_id
    JOIN data_security.ds_groups AS end_node ON rel.group_id = end_node.group_id
    WHERE rel.member_type = 'User'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.group_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.group_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.member_id, rel.group_id))) as path_edges,
        vp.start_name as start_name
    FROM vlp_u_g vp
    JOIN data_security.ds_memberships AS rel ON vp.end_id = rel.member_id
    JOIN data_security.ds_users AS end_node ON rel.group_id = end_node.group_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(rel.member_id, rel.group_id))
      AND rel.member_type = 'User'
)
SELECT 
      t.start_name AS `u.name`, 
      count(DISTINCT t.end_group_id) AS `groups_reached`
FROM vlp_u_g AS t
GROUP BY t.start_name
ORDER BY groups_reached DESC
