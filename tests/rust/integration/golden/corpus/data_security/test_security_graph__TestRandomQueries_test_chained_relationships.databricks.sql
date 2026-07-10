SELECT 
      u.name AS `u.name`, 
      g1.name AS `direct`, 
      g2.name AS `parent`
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = u.user_id AND t0.member_type = 'User'
INNER JOIN data_security.ds_groups AS g1 ON g1.group_id = t0.group_id
INNER JOIN data_security.ds_memberships AS t1 ON t1.member_id = g1.group_id AND t1.member_type = 'Group'
INNER JOIN data_security.ds_groups AS g2 ON g2.group_id = t1.group_id
ORDER BY u.name ASC
