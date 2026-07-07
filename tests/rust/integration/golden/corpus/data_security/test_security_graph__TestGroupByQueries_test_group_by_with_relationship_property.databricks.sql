SELECT 
      a.privilege AS `a.privilege`, 
      count(a.object_id) AS `file_count`
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_permissions AS a ON a.subject_id = u.user_id AND a.subject_type = 'User' AND a.object_type = 'File'
GROUP BY a.privilege
ORDER BY a.privilege ASC
