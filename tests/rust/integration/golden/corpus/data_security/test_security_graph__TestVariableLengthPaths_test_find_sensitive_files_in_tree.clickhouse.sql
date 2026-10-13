WITH RECURSIVE vlp_v1_nodes AS (
SELECT 
      'File' AS "__cg_label", 
      e.fs_id AS "__cg_id_0", 
      concat('File', ':', toString(e.fs_id)) AS "__cg_key"
FROM data_security.ds_fs_objects AS e
WHERE ((e."fs_type" = 'File'))

UNION ALL
SELECT 
      'Folder' AS "__cg_label", 
      e.fs_id AS "__cg_id_0", 
      concat('Folder', ':', toString(e.fs_id)) AS "__cg_key"
FROM data_security.ds_fs_objects AS e
WHERE ((e."fs_type" = 'Folder'))
), 
v1_rels AS (
SELECT 
      'CONTAINS' AS "__cg_type", 
      'Folder' AS "__cg_from_label", 
      'File' AS "__cg_to_label", 
      e.parent_id AS "__cg_rid_0", 
      e.fs_id AS "__cg_rid_1", 
      e.parent_id AS "__cg_from_0", 
      e.fs_id AS "__cg_to_0", 
      e.parent_id AS "__cg_start_0", 
      e.fs_id AS "__cg_end_0", 
      'Folder' AS "__cg_start_label", 
      'File' AS "__cg_end_label", 
      concat('CONTAINS:Folder:File:', toString(tuple(e.parent_id, e.fs_id))) AS "__cg_key"
FROM data_security.ds_fs_objects AS e
WHERE ((e."fs_type" = 'File'))

UNION ALL
SELECT 
      'CONTAINS' AS "__cg_type", 
      'Folder' AS "__cg_from_label", 
      'Folder' AS "__cg_to_label", 
      e.parent_id AS "__cg_rid_0", 
      e.fs_id AS "__cg_rid_1", 
      e.parent_id AS "__cg_from_0", 
      e.fs_id AS "__cg_to_0", 
      e.parent_id AS "__cg_start_0", 
      e.fs_id AS "__cg_end_0", 
      'Folder' AS "__cg_start_label", 
      'Folder' AS "__cg_end_label", 
      concat('CONTAINS:Folder:Folder:', toString(tuple(e.parent_id, e.fs_id))) AS "__cg_key"
FROM data_security.ds_fs_objects AS e
WHERE ((e."fs_type" = 'Folder'))
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS UInt32) AS hop_count, CAST([], 'Array(String)') AS path_edges, [start_node.__cg_key] AS path_nodes
    FROM vlp_v1_nodes AS start_node
    WHERE (start_node.__cg_label = 'Folder') AND ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      'Folder' AS "label", 
      v0.fs_id AS "id"
FROM data_security.ds_fs_objects AS v0
WHERE v0.name = 'root'))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS UInt32) AS hop_count, arrayConcat(vp.path_edges, [rel.__cg_key]) AS path_edges, arrayConcat(vp.path_nodes, [end_node.__cg_key]) AS path_nodes
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 5
      AND NOT has(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v1_trails
    WHERE hop_count >= 1
)
SELECT 
      v2.name AS "f.name", 
      v2.path AS "f.path"
FROM vlp_v1_path AS v1
JOIN data_security.ds_fs_objects AS v0 ON v1.start_id = v0.fs_id
JOIN data_security.ds_fs_objects AS v2 ON v1.end_id = v2.fs_id
WHERE (((v0."fs_type" = 'Folder')) AND v1.end_label = 'File' AND ((v2."fs_type" = 'File')) AND (v0.name = 'root' AND v2.sensitive_data = 1))
ORDER BY v2.name ASC
