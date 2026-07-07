SELECT 
      u.name AS `u.name`, 
      g.name AS `g.name`
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = u.user_id AND t0.member_type = 'User'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
