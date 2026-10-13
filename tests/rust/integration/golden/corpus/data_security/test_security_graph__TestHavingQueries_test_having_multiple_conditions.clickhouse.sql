WITH v2_labels AS (
SELECT 
      'File' AS "__cg_label", 
      e.fs_id AS "__cg_id_0"
FROM data_security.ds_fs_objects AS e
WHERE ((e."fs_type" = 'File'))

UNION ALL
SELECT 
      'Folder' AS "__cg_label", 
      e.fs_id AS "__cg_id_0"
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
with_w3 AS (
SELECT 
      v0.name AS "v3", 
      count(v2.__cg_label) AS "v4"
FROM data_security.ds_fs_objects AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.fs_id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
WHERE ((v0."fs_type" = 'Folder'))
GROUP BY v0.name
HAVING (count(v2.__cg_label) >= 1 AND count(v2.__cg_label) <= 10)
)
SELECT 
      w3.v3 AS "folder_name", 
      w3.v4 AS "item_count"
FROM with_w3 AS w3
ORDER BY w3.v4 DESC NULLS FIRST
