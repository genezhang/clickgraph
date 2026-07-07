SELECT 
      u.name AS `u.name`, 
      f.name AS `direct_file`, 
      r.privilege AS `r.privilege`
FROM data_security.ds_users AS u
LEFT JOIN (SELECT * FROM data_security.ds_permissions WHERE subject_type = 'User' AND object_type = 'File') AS r ON r.subject_id = u.user_id
LEFT JOIN data_security.ds_fs_objects AS f ON f.fs_id = r.object_id
ORDER BY u.name ASC
