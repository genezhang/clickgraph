SELECT 
      count(v0.fs_id) AS "sensitive_count"
FROM data_security.ds_fs_objects AS v0
WHERE (((v0."fs_type" = 'File')) AND v0.sensitive_data = 1)
