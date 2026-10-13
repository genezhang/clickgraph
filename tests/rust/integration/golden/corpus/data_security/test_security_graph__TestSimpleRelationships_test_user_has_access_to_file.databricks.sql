SELECT 
      v0.name AS `u.name`, 
      v1.privilege AS `r.privilege`, 
      v2.name AS `f.name`
FROM data_security.ds_users AS v0
JOIN data_security.ds_permissions AS v1 ON v1.subject_id = v0.user_id
JOIN data_security.ds_fs_objects AS v2 ON v1.object_id = v2.fs_id
WHERE ((((v1.`subject_type` = 'User') AND (v1.`object_type` = 'File'))) AND ((v2.`fs_type` = 'File')))
ORDER BY v0.name ASC NULLS LAST
