SELECT 
      f.name AS `f.name`, 
      f.path AS `f.path`
FROM data_security.ds_fs_objects AS f
ORDER BY f.path ASC
