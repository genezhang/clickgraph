SELECT 
      folder.name AS `folder.name`, 
      count(t0.fs_id) AS `file_count`
FROM data_security.ds_fs_objects AS folder
LEFT JOIN (SELECT * FROM data_security.ds_fs_objects WHERE fs_type = 'File') AS t0 ON t0.parent_id = folder.fs_id
GROUP BY folder.name
ORDER BY folder.name ASC
