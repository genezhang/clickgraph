SELECT 
      v0.name AS `u.name`, 
      v2.name AS `f.name`, 
      count(*) AS `access_count`
FROM data_security.ds_users AS v0
JOIN data_security.ds_permissions AS v1 ON v1.subject_id = v0.user_id
JOIN data_security.ds_fs_objects AS v2 ON v1.object_id = v2.fs_id
WHERE ((((v1.`subject_type` = 'User') AND (v1.`object_type` = 'Folder'))) AND ((v2.`fs_type` = 'Folder')))
GROUP BY v0.name, v2.name
ORDER BY v0.name ASC NULLS LAST, v2.name ASC NULLS LAST

-- column u.name: Value [("u.name", "u.name")]
-- column f.name: Value [("f.name", "f.name")]
-- column access_count: Value [("access_count", "access_count")]