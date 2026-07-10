SELECT `f.name` AS "f.name", count(coalesce(`child.fs_id`, `child.group_id`, `child.user_id`)) AS "children_count" FROM (
SELECT 
      NULL AS "department",
      NULL AS "description",
      NULL AS "email",
      NULL AS "exposure",
      toString(child.fs_id) AS "fs_id",
      NULL AS "group_id",
      toString(child.name) AS "name",
      toString(child.parent_id) AS "parent_id",
      toString(child.path) AS "path",
      toString(child.sensitive_data) AS "sensitive_data",
      NULL AS "user_id",
      f.name AS "f.name",
      toString(child.fs_id) AS "child.fs_id",
      NULL AS "child.group_id",
      NULL AS "child.user_id"
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_fs_objects AS child ON child.fs_id = t0.fs_id
UNION ALL 
SELECT 
      NULL AS "department",
      toString(child.description) AS "description",
      NULL AS "email",
      NULL AS "exposure",
      toString(f.fs_id) AS "fs_id",
      toString(child.group_id) AS "group_id",
      toString(child.name) AS "name",
      toString(f.parent_id) AS "parent_id",
      toString(f.path) AS "path",
      NULL AS "sensitive_data",
      NULL AS "user_id",
      f.name AS "f.name",
      toString(f.fs_id) AS "child.fs_id",
      toString(child.group_id) AS "child.group_id",
      NULL AS "child.user_id"
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'Group'
INNER JOIN data_security.ds_groups AS child ON child.group_id = t0.fs_id
UNION ALL 
SELECT 
      toString(child.department) AS "department",
      NULL AS "description",
      toString(child.email) AS "email",
      toString(child.exposure) AS "exposure",
      toString(f.fs_id) AS "fs_id",
      NULL AS "group_id",
      toString(child.name) AS "name",
      toString(f.parent_id) AS "parent_id",
      toString(f.path) AS "path",
      NULL AS "sensitive_data",
      toString(child.user_id) AS "user_id",
      f.name AS "f.name",
      toString(f.fs_id) AS "child.fs_id",
      NULL AS "child.group_id",
      toString(child.user_id) AS "child.user_id"
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'User'
INNER JOIN data_security.ds_users AS child ON child.user_id = t0.fs_id
) AS __union
GROUP BY `f.name`
ORDER BY children_count DESC
