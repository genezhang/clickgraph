SELECT 
      v0.name AS "folder.name", 
      count(v2.fs_id) AS "file_count"
FROM data_security.ds_fs_objects AS v0
JOIN data_security.ds_fs_objects AS v1 ON v1.parent_id = v0.fs_id
JOIN data_security.ds_fs_objects AS v2 ON v1.fs_id = v2.fs_id
WHERE (((v0."fs_type" = 'Folder')) AND ((v1."fs_type" = 'File')) AND ((v2."fs_type" = 'File')))
GROUP BY v0.name
ORDER BY count(v2.fs_id) DESC NULLS FIRST

-- column folder.name: Value [("folder.name", "folder.name")]
-- column file_count: Value [("file_count", "file_count")]