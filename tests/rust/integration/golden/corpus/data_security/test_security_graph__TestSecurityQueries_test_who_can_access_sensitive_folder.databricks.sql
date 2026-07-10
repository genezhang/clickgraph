SELECT 
      g.name AS `g.name`
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_permissions AS t0 ON f.fs_id = t0.object_id AND t0.subject_type = 'Group' AND t0.object_type = 'Folder'
INNER JOIN data_security.ds_groups AS g ON t0.subject_id = g.group_id
WHERE f.name = 'secrets'
