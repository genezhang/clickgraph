WITH optional_o1 AS (
SELECT 
      v1.parent_id AS "v0__fs_id", 
      v2.fs_id AS "v2__fs_id"
FROM data_security.ds_fs_objects AS v1
JOIN data_security.ds_fs_objects AS v2 ON v1.fs_id = v2.fs_id
WHERE (((v1."fs_type" = 'File')) AND ((v2."fs_type" = 'File')))
)
SELECT 
      v0.name AS "folder.name", 
      count(o1.v2__fs_id) AS "file_count"
FROM data_security.ds_fs_objects AS v0
LEFT JOIN optional_o1 AS o1 ON v0.fs_id = o1.v0__fs_id
WHERE ((v0."fs_type" = 'Folder'))
GROUP BY v0.name
ORDER BY v0.name ASC

-- column folder.name: Value [("folder.name", "folder.name")]
-- column file_count: Value [("file_count", "file_count")]