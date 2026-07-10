SELECT 
      f.name AS "f.name"
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'Folder'
