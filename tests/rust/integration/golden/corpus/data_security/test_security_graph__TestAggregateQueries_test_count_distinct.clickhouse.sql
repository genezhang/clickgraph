SELECT 
      count(DISTINCT u.user_id) AS "unique_users"
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_permissions AS t0 ON t0.subject_id = u.user_id AND t0.subject_type = 'User' AND t0.object_type = 'File'
