SELECT 
      u.name AS "u.name", 
      f.name AS "f.name", 
      count(*) AS "access_count"
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_permissions AS t0 ON t0.subject_id = u.user_id AND t0.subject_type = 'User' AND t0.object_type = 'Folder'
INNER JOIN data_security.ds_fs_objects AS f ON f.fs_id = t0.object_id
GROUP BY u.name, f.name
ORDER BY u.name ASC, f.name ASC
