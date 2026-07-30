SELECT 
      NULL AS "member.department", 
      toString(member.description) AS "member.description", 
      NULL AS "member.email", 
      NULL AS "member.exposure", 
      toString(member.group_id) AS "member.group_id", 
      toString(member.name) AS "member.name", 
      NULL AS "member.user_id"
FROM data_security.ds_groups AS member
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = member.group_id AND t0.member_type = 'Group'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
WHERE g.name = 'Engineering'
UNION ALL 
SELECT 
      toString(member.department) AS "member.department", 
      NULL AS "member.description", 
      toString(member.email) AS "member.email", 
      toString(member.exposure) AS "member.exposure", 
      NULL AS "member.group_id", 
      toString(member.name) AS "member.name", 
      toString(member.user_id) AS "member.user_id"
FROM data_security.ds_groups AS g
INNER JOIN data_security.ds_memberships AS t0 ON g.group_id = t0.group_id AND t0.member_type = 'User'
INNER JOIN data_security.ds_users AS member ON t0.member_id = member.user_id
WHERE g.name = 'Engineering'
