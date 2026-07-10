SELECT 
      NULL AS "member.department", 
      NULL AS "member.description", 
      NULL AS "member.email", 
      NULL AS "member.exposure", 
      toString(member.fs_id) AS "member.fs_id", 
      NULL AS "member.group_id", 
      toString(member.name) AS "member.name", 
      toString(member.parent_id) AS "member.parent_id", 
      toString(member.path) AS "member.path", 
      toString(member.sensitive_data) AS "member.sensitive_data", 
      NULL AS "member.user_id"
FROM data_security.ds_fs_objects AS member
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = member.fs_id AND t0.member_type = 'File'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
WHERE g.name = 'Engineering'
UNION ALL 
SELECT 
      NULL AS "member.department", 
      toString(member.description) AS "member.description", 
      NULL AS "member.email", 
      NULL AS "member.exposure", 
      NULL AS "member.fs_id", 
      toString(member.group_id) AS "member.group_id", 
      toString(member.name) AS "member.name", 
      NULL AS "member.parent_id", 
      NULL AS "member.path", 
      NULL AS "member.sensitive_data", 
      NULL AS "member.user_id"
FROM data_security.ds_groups AS g
INNER JOIN data_security.ds_memberships AS t0 ON g.group_id = t0.group_id AND t0.member_type = 'File'
INNER JOIN data_security.ds_groups AS member ON t0.member_id = member.fs_id
WHERE g.name = 'Engineering'
UNION ALL 
SELECT 
      toString(member.department) AS "member.department", 
      NULL AS "member.description", 
      toString(member.email) AS "member.email", 
      toString(member.exposure) AS "member.exposure", 
      NULL AS "member.fs_id", 
      NULL AS "member.group_id", 
      toString(member.name) AS "member.name", 
      NULL AS "member.parent_id", 
      NULL AS "member.path", 
      NULL AS "member.sensitive_data", 
      toString(member.user_id) AS "member.user_id"
FROM data_security.ds_groups AS g
INNER JOIN data_security.ds_memberships AS t0 ON g.group_id = t0.group_id AND t0.member_type = 'File'
INNER JOIN data_security.ds_users AS member ON t0.member_id = member.fs_id
WHERE g.name = 'Engineering'
