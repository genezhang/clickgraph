WITH with_file_access_count_user_name_cte_0 AS (SELECT 
      u.name AS `user_name`, 
      count(t0.object_id) AS `file_access_count`
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_permissions AS t0 ON t0.subject_id = u.user_id AND t0.subject_type = 'User' AND t0.object_type = 'File'
GROUP BY u.name
HAVING file_access_count > 0
)
SELECT 
      file_access_count_user_name.user_name AS `user_name`, 
      file_access_count_user_name.file_access_count AS `file_access_count`
FROM with_file_access_count_user_name_cte_0 AS file_access_count_user_name
ORDER BY file_access_count_user_name.file_access_count DESC
