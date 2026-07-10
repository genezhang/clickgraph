SELECT 
      f.sensitive_data AS `f.sensitive_data`, 
      count(f.fs_id) AS `count`
FROM data_security.ds_fs_objects AS f
GROUP BY f.sensitive_data
ORDER BY f.sensitive_data ASC
