WITH RECURSIVE vlp_u_g AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.group_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.group_id) as path_nodes,
        start_node.exposure as start_exposure,
        start_node.name as start_name,
        end_node.name as end_name
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
        vp.start_exposure as start_exposure,
        vp.start_name as start_name,
        end_node.name as end_name
    FROM vlp_u_g vp
    JOIN data_security.ds_memberships AS rel ON vp.end_id = rel.member_id
    JOIN data_security.ds_users AS end_node ON rel.group_id = end_node.group_id
    WHERE vp.hop_count < 5
      AND NOT array_contains(vp.path_nodes, end_node.group_id)
      AND rel.member_type = 'User'
)
SELECT `u.name` AS `u.name`, `via_group` AS `via_group` FROM (
SELECT DISTINCT 
      t.start_name AS `u.name`, 
      t.end_name AS `via_group`, 
      t.start_name AS `__order_col_0`
FROM vlp_u_g AS t
INNER JOIN data_security.ds_permissions AS t0 ON t0.subject_id = t.end_id AND (t0.subject_type = 'Group' AND t0.object_type = 'File')
WHERE t.start_exposure = 'external'
UNION ALL 
SELECT DISTINCT 
      t.start_name AS `u.name`, 
      t.end_name AS `via_group`, 
      t.start_name AS `__order_col_0`
FROM vlp_u_g AS t
INNER JOIN data_security.ds_permissions AS t0 ON t0.subject_id = t.end_id AND (t0.subject_type = 'Group' AND t0.object_type = 'File')
WHERE t.start_exposure = 'external'
UNION ALL 
SELECT DISTINCT 
      t.start_name AS `u.name`, 
      t.end_name AS `via_group`, 
      t.start_name AS `__order_col_0`
FROM vlp_u_g AS t
INNER JOIN data_security.ds_permissions AS t0 ON t0.subject_id = t.end_id AND (t0.subject_type = 'Group' AND t0.object_type = 'File')
WHERE t.start_exposure = 'external'
) AS __union
ORDER BY __union.`__order_col_0` ASC
