SELECT 
      g1.name AS "child", 
      g2.name AS "parent"
FROM data_security.ds_groups AS g1
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = g1.group_id AND t0.member_type = 'Group'
INNER JOIN data_security.ds_groups AS g2 ON g2.group_id = t0.group_id
ORDER BY g1.name ASC
