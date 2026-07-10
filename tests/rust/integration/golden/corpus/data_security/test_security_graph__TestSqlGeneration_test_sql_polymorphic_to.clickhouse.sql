SELECT 
      NULL AS "child.department", 
      NULL AS "child.description", 
      NULL AS "child.email", 
      NULL AS "child.exposure", 
      toString(child.fs_id) AS "child.fs_id", 
      NULL AS "child.group_id", 
      toString(child.name) AS "child.name", 
      toString(child.parent_id) AS "child.parent_id", 
      toString(child.path) AS "child.path", 
      toString(child.sensitive_data) AS "child.sensitive_data", 
      NULL AS "child.user_id"
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_fs_objects AS child ON child.fs_id = t0.fs_id
WHERE f.name = 'docs'
UNION ALL 
SELECT 
      NULL AS "child.department", 
      toString(child.description) AS "child.description", 
      NULL AS "child.email", 
      NULL AS "child.exposure", 
      NULL AS "child.fs_id", 
      toString(child.group_id) AS "child.group_id", 
      toString(child.name) AS "child.name", 
      NULL AS "child.parent_id", 
      NULL AS "child.path", 
      NULL AS "child.sensitive_data", 
      NULL AS "child.user_id"
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_groups AS child ON child.fs_id = t0.fs_id
WHERE f.name = 'docs'
UNION ALL 
SELECT 
      toString(child.department) AS "child.department", 
      NULL AS "child.description", 
      toString(child.email) AS "child.email", 
      toString(child.exposure) AS "child.exposure", 
      NULL AS "child.fs_id", 
      NULL AS "child.group_id", 
      toString(child.name) AS "child.name", 
      NULL AS "child.parent_id", 
      NULL AS "child.path", 
      NULL AS "child.sensitive_data", 
      toString(child.user_id) AS "child.user_id"
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_users AS child ON child.fs_id = t0.fs_id
WHERE f.name = 'docs'
