SELECT 
      f1.name AS `parent`, 
      f2.name AS `child`
FROM data_security.ds_fs_objects AS f1
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f1.fs_id AND t0.fs_type = 'Folder'
INNER JOIN data_security.ds_fs_objects AS f2 ON f2.fs_id = t0.fs_id
ORDER BY f1.name ASC
