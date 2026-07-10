SELECT 
      folder.name AS `folder.name`, 
      file.name AS `file.name`
FROM data_security.ds_fs_objects AS folder
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = folder.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_fs_objects AS file ON file.fs_id = t0.fs_id
ORDER BY folder.name ASC, file.name ASC
