SELECT 
      g.name AS `g.name`
FROM data_security.ds_groups AS g
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = g.group_id AND t0.member_type = 'Group'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
