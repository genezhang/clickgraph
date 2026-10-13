SELECT 
      v1.privilege AS "a.privilege", 
      count(v2.fs_id) AS "file_count"
FROM data_security.ds_users AS v0
JOIN data_security.ds_permissions AS v1 ON v1.subject_id = v0.user_id
JOIN data_security.ds_fs_objects AS v2 ON v1.object_id = v2.fs_id
WHERE ((((v1."subject_type" = 'User') AND (v1."object_type" = 'File'))) AND ((v2."fs_type" = 'File')))
GROUP BY v1.privilege
ORDER BY v1.privilege ASC

-- column a.privilege: Value [("a.privilege", "a.privilege")]
-- column file_count: Value [("file_count", "file_count")]