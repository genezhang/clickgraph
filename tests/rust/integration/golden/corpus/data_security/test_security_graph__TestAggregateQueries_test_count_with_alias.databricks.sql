WITH v2_labels AS (
SELECT 
      'File' AS `__cg_label`, 
      e.fs_id AS `__cg_id_0`
FROM data_security.ds_fs_objects AS e
WHERE ((e.`fs_type` = 'File'))

UNION ALL
SELECT 
      'Folder' AS `__cg_label`, 
      e.fs_id AS `__cg_id_0`
FROM data_security.ds_fs_objects AS e
WHERE ((e.`fs_type` = 'Folder'))
), 
v1_rels AS (
SELECT 
      'CONTAINS' AS `__cg_type`, 
      'Folder' AS `__cg_from_label`, 
      'File' AS `__cg_to_label`, 
      e.parent_id AS `__cg_rid_0`, 
      e.fs_id AS `__cg_rid_1`, 
      e.parent_id AS `__cg_from_0`, 
      e.fs_id AS `__cg_to_0`, 
      e.parent_id AS `__cg_start_0`, 
      e.fs_id AS `__cg_end_0`, 
      'Folder' AS `__cg_start_label`, 
      'File' AS `__cg_end_label`, 
      concat('CONTAINS:Folder:File:', to_json(named_struct('_1', CAST(e.parent_id AS STRING), '_2', CAST(e.fs_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM data_security.ds_fs_objects AS e
WHERE ((e.`fs_type` = 'File'))

UNION ALL
SELECT 
      'CONTAINS' AS `__cg_type`, 
      'Folder' AS `__cg_from_label`, 
      'Folder' AS `__cg_to_label`, 
      e.parent_id AS `__cg_rid_0`, 
      e.fs_id AS `__cg_rid_1`, 
      e.parent_id AS `__cg_from_0`, 
      e.fs_id AS `__cg_to_0`, 
      e.parent_id AS `__cg_start_0`, 
      e.fs_id AS `__cg_end_0`, 
      'Folder' AS `__cg_start_label`, 
      'Folder' AS `__cg_end_label`, 
      concat('CONTAINS:Folder:Folder:', to_json(named_struct('_1', CAST(e.parent_id AS STRING), '_2', CAST(e.fs_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM data_security.ds_fs_objects AS e
WHERE ((e.`fs_type` = 'Folder'))
)
SELECT 
      v0.name AS `f.name`, 
      count(v2.__cg_label) AS `children_count`
FROM data_security.ds_fs_objects AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.fs_id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
WHERE ((v0.`fs_type` = 'Folder'))
GROUP BY v0.name
ORDER BY count(v2.__cg_label) DESC NULLS FIRST
