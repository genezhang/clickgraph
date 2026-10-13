SELECT 
      count(v0.fs_id) AS "total_files", 
      count(DISTINCT v0.path) AS "unique_paths"
FROM data_security.ds_fs_objects AS v0
WHERE ((v0."fs_type" = 'File'))

-- column total_files: Value [("total_files", "total_files")]
-- column unique_paths: Value [("unique_paths", "unique_paths")]