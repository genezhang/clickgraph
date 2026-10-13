SELECT 
      v2.name AS "g.name", 
      count(DISTINCT v4.fs_id) AS "accessible_files"
FROM data_security.ds_users AS v0
JOIN data_security.ds_memberships AS v1 ON v1.member_id = v0.user_id
JOIN data_security.ds_groups AS v2 ON v1.group_id = v2.group_id
JOIN data_security.ds_permissions AS v3 ON v3.subject_id = v0.user_id
JOIN data_security.ds_fs_objects AS v4 ON v3.object_id = v4.fs_id
WHERE (((v1."member_type" = 'User')) AND (((v3."subject_type" = 'User') AND (v3."object_type" = 'File'))) AND ((v4."fs_type" = 'File')))
GROUP BY v2.name
ORDER BY count(DISTINCT v4.fs_id) DESC NULLS FIRST
