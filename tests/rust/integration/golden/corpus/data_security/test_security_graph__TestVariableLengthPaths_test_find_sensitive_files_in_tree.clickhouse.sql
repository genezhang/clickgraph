WITH RECURSIVE vlp_root_f_inner AS (
    SELECT 
        start_node.fs_id as start_id,
        end_node.fs_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.fs_id, end_node.fs_id] as path_nodes,
        [tuple(rel.parent_id, rel.fs_id)] as path_edges,
        end_node.name as end_name,
        end_node.path as end_path,
        end_node.sensitive_data as end_sensitive_data
    FROM data_security.ds_fs_objects AS start_node
    JOIN data_security.ds_fs_objects AS rel ON start_node.fs_id = rel.parent_id
    JOIN data_security.ds_fs_objects AS end_node ON rel.fs_id = end_node.fs_id
    WHERE rel.fs_type = 'File' AND start_node.name = 'root'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.fs_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.fs_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.parent_id, rel.fs_id)]) as path_edges,
        end_node.name as end_name,
        end_node.path as end_path,
        end_node.sensitive_data as end_sensitive_data
    FROM vlp_root_f_inner vp
    JOIN data_security.ds_fs_objects AS rel ON vp.end_id = rel.parent_id
    JOIN data_security.ds_fs_objects AS end_node ON rel.fs_id = end_node.fs_id
    WHERE vp.hop_count < 5
      AND NOT has(vp.path_edges, tuple(rel.parent_id, rel.fs_id))
      AND rel.fs_type = 'File'
),
vlp_root_f AS (
    SELECT * FROM vlp_root_f_inner WHERE (end_sensitive_data = 1)
)
SELECT 
      t.end_name AS "f.name", 
      t.end_path AS "f.path"
FROM vlp_root_f AS t
ORDER BY t.end_name ASC
