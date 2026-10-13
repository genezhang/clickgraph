SELECT 
      v0.name AS `f.name`, 
      v2.name AS `folder.name`
FROM data_security.ds_fs_objects AS v0
JOIN data_security.ds_fs_objects AS v1 ON v1.fs_id = v0.fs_id
JOIN data_security.ds_fs_objects AS v2 ON v1.parent_id = v2.fs_id
WHERE (((v0.`fs_type` = 'File')) AND ((v1.`fs_type` = 'File')) AND ((v2.`fs_type` = 'Folder')))
ORDER BY v0.name ASC NULLS LAST
LIMIT 5
-- column f.name: Value [("f.name", "f.name")]
-- column folder.name: Value [("folder.name", "folder.name")]