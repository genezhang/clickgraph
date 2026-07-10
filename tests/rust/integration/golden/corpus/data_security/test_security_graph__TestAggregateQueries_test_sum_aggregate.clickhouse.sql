SELECT 
      count(f.fs_id) AS "sensitive_count"
FROM data_security.ds_fs_objects AS f
WHERE f.sensitive_data = 1
