SELECT `g.name` AS `g.name` FROM (
SELECT DISTINCT 
      g.name AS `g.name`, 
      g.name AS `__order_col_0`
FROM data_security.ds_fs_objects AS m
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = m.fs_id AND t0.member_type = 'File'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
UNION ALL 
SELECT DISTINCT 
      g.name AS `g.name`, 
      g.name AS `__order_col_0`
FROM data_security.ds_groups AS m
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = m.fs_id AND t0.member_type = 'File'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
UNION ALL 
SELECT DISTINCT 
      g.name AS `g.name`, 
      g.name AS `__order_col_0`
FROM data_security.ds_users AS m
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = m.fs_id AND t0.member_type = 'File'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
) AS __union
ORDER BY __union.`__order_col_0` ASC
