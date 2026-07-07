SELECT 
      folder.name AS `folder.name`, 
      count(t0.fs_id) AS `file_count`
FROM data_security.ds_fs_objects AS folder
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = folder.fs_id AND t0.fs_type = 'File'
GROUP BY folder.name
ORDER BY file_count DESC
