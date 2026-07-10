SELECT 
      f.name AS `f.name`, 
      f.path AS `f.path`
FROM data_security.ds_fs_objects AS f
WHERE f.sensitive_data = 1
ORDER BY f.name ASC
