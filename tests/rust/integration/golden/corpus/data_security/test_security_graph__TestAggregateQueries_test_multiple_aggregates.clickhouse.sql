SELECT 
      count(f.fs_id) AS "total_files", 
      count(DISTINCT f.path) AS "unique_paths"
FROM data_security.ds_fs_objects AS f
