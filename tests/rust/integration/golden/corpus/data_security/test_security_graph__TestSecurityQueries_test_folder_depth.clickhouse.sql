WITH RECURSIVE vlp_root_f AS (
    SELECT 
        start_node.fs_id as start_id,
        end_node.fs_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.fs_id, end_node.fs_id] as path_nodes,
        end_node.name as end_name,
        end_node.path as end_path
    FROM data_security.ds_fs_objects AS start_node
    JOIN data_security.ds_fs_objects AS rel ON start_node.fs_id = rel.parent_id
    JOIN data_security.ds_fs_objects AS end_node ON rel.fs_id = end_node.fs_id
    WHERE rel.fs_type = 'Folder' AND start_node.name = 'root'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.fs_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.fs_id]) as path_nodes,
        end_node.name as end_name,
        end_node.path as end_path
    FROM vlp_root_f vp
    JOIN data_security.ds_fs_objects AS rel ON vp.end_id = rel.parent_id
    JOIN data_security.ds_fs_objects AS end_node ON rel.fs_id = end_node.fs_id
    WHERE vp.hop_count < 5
      AND NOT has(vp.path_nodes, end_node.fs_id)
      AND rel.fs_type = 'Folder'
)
SELECT 
      t.end_name AS "f.name", 
      t.end_path AS "f.path"
FROM vlp_root_f AS t
ORDER BY t.end_path ASC
