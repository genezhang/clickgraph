WITH optional_o1 AS (
SELECT 
      v1.subject_id AS "v0__user_id", 
      v1.subject_id AS "v1__subject_id", 
      v1.object_id AS "v1__object_id", 
      v1.privilege AS "p2_v1_privilege", 
      v2.fs_id AS "v2__fs_id", 
      v2.name AS "p2_v2_name"
FROM data_security.ds_permissions AS v1
JOIN data_security.ds_fs_objects AS v2 ON v1.object_id = v2.fs_id
WHERE ((((v1."subject_type" = 'User') AND (v1."object_type" = 'File'))) AND ((v2."fs_type" = 'File')))
)
SELECT 
      v0.name AS "u.name", 
      o1.p2_v2_name AS "direct_file", 
      o1.p2_v1_privilege AS "r.privilege"
FROM data_security.ds_users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
ORDER BY v0.name ASC
