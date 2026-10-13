SELECT 
      count(DISTINCT v0.user_id) AS `unique_users`
FROM data_security.ds_users AS v0
JOIN data_security.ds_permissions AS v1 ON v1.subject_id = v0.user_id
JOIN data_security.ds_fs_objects AS v2 ON v1.object_id = v2.fs_id
WHERE ((((v1.`subject_type` = 'User') AND (v1.`object_type` = 'File'))) AND ((v2.`fs_type` = 'File')))

-- column unique_users: Value [("unique_users", "unique_users")]