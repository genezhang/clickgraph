SELECT 
      NULL AS `child.department`, 
      NULL AS `child.description`, 
      NULL AS `child.email`, 
      NULL AS `child.exposure`, 
      string(child.fs_id) AS `child.fs_id`, 
      NULL AS `child.group_id`, 
      string(child.name) AS `child.name`, 
      string(child.parent_id) AS `child.parent_id`, 
      string(child.path) AS `child.path`, 
      string(child.sensitive_data) AS `child.sensitive_data`, 
      NULL AS `child.user_id`
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_fs_objects AS child ON child.fs_id = t0.fs_id
WHERE f.name = 'docs'
UNION ALL 
SELECT 
      NULL AS `child.department`, 
      string(child.description) AS `child.description`, 
      NULL AS `child.email`, 
      NULL AS `child.exposure`, 
      NULL AS `child.fs_id`, 
      string(child.group_id) AS `child.group_id`, 
      string(child.name) AS `child.name`, 
      NULL AS `child.parent_id`, 
      NULL AS `child.path`, 
      NULL AS `child.sensitive_data`, 
      NULL AS `child.user_id`
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_groups AS child ON child.fs_id = t0.fs_id
WHERE f.name = 'docs'
UNION ALL 
SELECT 
      string(child.department) AS `child.department`, 
      NULL AS `child.description`, 
      string(child.email) AS `child.email`, 
      string(child.exposure) AS `child.exposure`, 
      NULL AS `child.fs_id`, 
      NULL AS `child.group_id`, 
      string(child.name) AS `child.name`, 
      NULL AS `child.parent_id`, 
      NULL AS `child.path`, 
      NULL AS `child.sensitive_data`, 
      string(child.user_id) AS `child.user_id`
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_users AS child ON child.fs_id = t0.fs_id
WHERE f.name = 'docs'
