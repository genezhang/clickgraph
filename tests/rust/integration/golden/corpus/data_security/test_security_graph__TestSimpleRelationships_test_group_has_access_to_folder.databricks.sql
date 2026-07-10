SELECT 
      g.name AS `g.name`, 
      r.privilege AS `r.privilege`, 
      f.name AS `f.name`
FROM data_security.ds_groups AS g
INNER JOIN data_security.ds_permissions AS r ON r.subject_id = g.group_id AND r.subject_type = 'Group' AND r.object_type = 'Folder'
INNER JOIN data_security.ds_fs_objects AS f ON f.fs_id = r.object_id
ORDER BY g.name ASC
