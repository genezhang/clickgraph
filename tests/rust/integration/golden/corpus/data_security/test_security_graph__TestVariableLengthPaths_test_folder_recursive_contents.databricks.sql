WITH RECURSIVE vlp_root_item AS (
    SELECT 
        start_node.fs_id as start_id,
        end_node.fs_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.fs_id, end_node.fs_id) as path_nodes,
        end_node.name as end_name,
        end_node.path as end_path
    FROM data_security.ds_fs_objects AS start_node
    JOIN data_security.ds_fs_objects AS rel ON start_node.fs_id = rel.parent_id
    JOIN data_security.ds_fs_objects AS end_node ON rel.fs_id = end_node.fs_id
    WHERE rel.fs_type = 'File' AND start_node.name = 'engineering'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.fs_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.fs_id)) as path_nodes,
        end_node.name as end_name,
        end_node.path as end_path
    FROM vlp_root_item vp
    JOIN data_security.ds_fs_objects AS rel ON vp.end_id = rel.parent_id
    JOIN data_security.ds_fs_objects AS end_node ON rel.fs_id = end_node.fs_id
    WHERE vp.hop_count < 5
      AND NOT array_contains(vp.path_nodes, end_node.fs_id)
      AND rel.fs_type = 'File'
)
SELECT 
      t.end_name AS `item.name`, 
      t.end_path AS `item.path`
FROM vlp_root_item AS t
ORDER BY t.end_path ASC
