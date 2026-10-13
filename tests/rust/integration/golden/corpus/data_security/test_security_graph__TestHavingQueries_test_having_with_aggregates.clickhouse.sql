WITH with_w1 AS (
SELECT 
      v0.name AS "v3", 
      count(v2.fs_id) AS "v4"
FROM data_security.ds_users AS v0
JOIN data_security.ds_permissions AS v1 ON v1.subject_id = v0.user_id
JOIN data_security.ds_fs_objects AS v2 ON v1.object_id = v2.fs_id
WHERE ((((v1."subject_type" = 'User') AND (v1."object_type" = 'File'))) AND ((v2."fs_type" = 'File')))
GROUP BY v0.name
HAVING count(v2.fs_id) > 0
)
SELECT 
      w1.v3 AS "user_name", 
      w1.v4 AS "file_access_count"
FROM with_w1 AS w1
ORDER BY w1.v4 DESC NULLS FIRST

-- column user_name: Value [("user_name", "user_name")]
-- column file_access_count: Value [("file_access_count", "file_access_count")]