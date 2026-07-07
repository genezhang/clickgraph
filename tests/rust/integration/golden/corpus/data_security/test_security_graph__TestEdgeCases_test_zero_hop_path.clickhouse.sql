WITH RECURSIVE vlp_u_target AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id] as path_nodes,
        '' as end_description,
        start_node.name as end_name
    FROM data_security.ds_users AS start_node
    WHERE start_node.name = 'Alice'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.group_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.group_id]) as path_nodes,
        end_node.description as end_description,
        end_node.name as end_name
    FROM vlp_u_target vp
    JOIN data_security.ds_memberships AS rel ON vp.end_id = rel.member_id
    JOIN data_security.ds_users AS end_node ON rel.group_id = end_node.group_id
    WHERE vp.hop_count < 1
      AND NOT has(vp.path_nodes, end_node.group_id)
      AND rel.member_type = 'User'
)
SELECT 
      t.end_description AS "target.description", 
      t.end_group_id AS "target.group_id", 
      t.end_name AS "target.name"
FROM vlp_u_target AS t
