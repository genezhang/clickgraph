WITH RECURSIVE vlp_u_g AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.group_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.group_id] as path_nodes,
        end_node.name as end_name
    FROM data_security.ds_users AS start_node
    JOIN data_security.ds_memberships AS rel ON start_node.user_id = rel.member_id
    JOIN data_security.ds_groups AS end_node ON rel.group_id = end_node.group_id
    WHERE rel.member_type = 'User' AND start_node.name = 'Alice'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.group_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.group_id]) as path_nodes,
        end_node.name as end_name
    FROM vlp_u_g vp
    JOIN data_security.ds_memberships AS rel ON vp.end_id = rel.member_id
    JOIN data_security.ds_users AS end_node ON rel.group_id = end_node.group_id
    WHERE vp.hop_count < 100
      AND NOT has(vp.path_nodes, end_node.group_id)
      AND rel.member_type = 'User'
)
SELECT DISTINCT 
      t.end_name AS "g.name"
FROM vlp_u_g AS t
