SELECT 
      child.fs_id AS `child.fs_id`, 
      child.name AS `child.name`, 
      child.parent_id AS `child.parent_id`, 
      child.path AS `child.path`, 
      child.sensitive_data AS `child.sensitive_data`
FROM data_security.ds_fs_objects AS f
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = f.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_fs_objects AS child ON child.fs_id = t0.fs_id
WHERE f.name = 'docs'
