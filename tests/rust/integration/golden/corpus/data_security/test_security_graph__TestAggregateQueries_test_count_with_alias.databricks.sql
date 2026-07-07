SELECT `f.name` AS `f.name`, count(`child.fs_id`) AS `children_count` FROM (
SELECT 
      NULL AS `department`,
      NULL AS `description`,
      NULL AS `email`,
      NULL AS `exposure`,
      string(child.fs_id) AS `fs_id`,
      NULL AS `group_id`,
      string(child.name) AS `name`,
      string(child.parent_id) AS `parent_id`,
      string(child.path) AS `path`,
      string(child.sensitive_data) AS `sensitive_data`,
      NULL AS `user_id`,
      f.name AS `f.name`,
      string(child.fs_id) AS `child.fs_id`
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_fs_objects AS child ON child.fs_id = t0.fs_id
UNION ALL 
SELECT 
      NULL AS `department`,
      string(child.description) AS `description`,
      NULL AS `email`,
      NULL AS `exposure`,
      string(f.fs_id) AS `fs_id`,
      string(child.group_id) AS `group_id`,
      string(child.name) AS `name`,
      string(f.parent_id) AS `parent_id`,
      string(f.path) AS `path`,
      NULL AS `sensitive_data`,
      NULL AS `user_id`,
      f.name AS `f.name`,
      string(f.fs_id) AS `child.fs_id`
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'Group'
INNER JOIN data_security.ds_groups AS child ON child.group_id = t0.fs_id
UNION ALL 
SELECT 
      string(child.department) AS `department`,
      NULL AS `description`,
      string(child.email) AS `email`,
      string(child.exposure) AS `exposure`,
      string(f.fs_id) AS `fs_id`,
      NULL AS `group_id`,
      string(child.name) AS `name`,
      string(f.parent_id) AS `parent_id`,
      string(f.path) AS `path`,
      NULL AS `sensitive_data`,
      string(child.user_id) AS `user_id`,
      f.name AS `f.name`,
      string(f.fs_id) AS `child.fs_id`
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'User'
INNER JOIN data_security.ds_users AS child ON child.user_id = t0.fs_id
) AS __union
GROUP BY `f.name`
ORDER BY children_count DESC
