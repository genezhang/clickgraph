SELECT 
      u.name AS `u.name`, 
      r.privilege AS `r.privilege`, 
      f.name AS `f.name`
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_permissions AS r ON r.subject_id = u.user_id AND r.subject_type = 'User' AND r.object_type = 'File'
INNER JOIN data_security.ds_fs_objects AS f ON f.fs_id = r.object_id
ORDER BY u.name ASC
