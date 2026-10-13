SELECT 
      v0.name AS "parent", 
      v2.name AS "child"
FROM data_security.ds_fs_objects AS v0
JOIN data_security.ds_fs_objects AS v1 ON v1.parent_id = v0.fs_id
JOIN data_security.ds_fs_objects AS v2 ON v1.fs_id = v2.fs_id
WHERE (((v0."fs_type" = 'Folder')) AND ((v1."fs_type" = 'Folder')) AND ((v2."fs_type" = 'Folder')))
ORDER BY v0.name ASC
