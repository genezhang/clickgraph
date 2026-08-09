SELECT `f.name` AS `f.name`, count(`child.fs_id`) AS `children_count` FROM (
SELECT 
      string(child.fs_id) AS `fs_id`,
      string(child.name) AS `name`,
      string(child.parent_id) AS `parent_id`,
      string(child.path) AS `path`,
      string(child.sensitive_data) AS `sensitive_data`,
      f.name AS `f.name`,
      string(child.fs_id) AS `child.fs_id`
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_fs_objects AS child ON child.fs_id = t0.fs_id
UNION ALL 
SELECT 
      string(child.fs_id) AS `fs_id`,
      string(child.name) AS `name`,
      string(child.parent_id) AS `parent_id`,
      string(child.path) AS `path`,
      NULL AS `sensitive_data`,
      f.name AS `f.name`,
      string(child.fs_id) AS `child.fs_id`
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'Folder'
INNER JOIN data_security.ds_fs_objects AS child ON child.fs_id = t0.fs_id
) AS __union
GROUP BY `f.name`
ORDER BY `children_count` DESC NULLS FIRST
