SELECT 
      NULL AS `member.department`, 
      string(member.description) AS `member.description`, 
      NULL AS `member.email`, 
      NULL AS `member.exposure`, 
      string(member.group_id) AS `member.group_id`, 
      string(member.name) AS `member.name`, 
      NULL AS `member.user_id`
FROM data_security.ds_groups AS member
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = member.group_id AND t0.member_type = 'Group'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
WHERE g.name = 'Engineering'
UNION ALL 
SELECT 
      string(member.department) AS `member.department`, 
      NULL AS `member.description`, 
      string(member.email) AS `member.email`, 
      string(member.exposure) AS `member.exposure`, 
      NULL AS `member.group_id`, 
      string(member.name) AS `member.name`, 
      string(member.user_id) AS `member.user_id`
FROM data_security.ds_groups AS g
INNER JOIN data_security.ds_memberships AS t0 ON g.group_id = t0.group_id AND t0.member_type = 'Group'
INNER JOIN data_security.ds_users AS member ON t0.member_id = member.group_id
WHERE g.name = 'Engineering'
