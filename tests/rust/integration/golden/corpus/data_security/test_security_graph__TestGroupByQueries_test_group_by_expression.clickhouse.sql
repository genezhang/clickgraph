SELECT 
      v0.sensitive_data AS "f.sensitive_data", 
      count(v0.fs_id) AS "count"
FROM data_security.ds_fs_objects AS v0
WHERE ((v0."fs_type" = 'File'))
GROUP BY v0.sensitive_data
ORDER BY v0.sensitive_data ASC
