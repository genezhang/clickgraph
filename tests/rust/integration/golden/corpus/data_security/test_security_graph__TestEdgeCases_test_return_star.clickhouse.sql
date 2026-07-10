SELECT 
      g.description AS "g.description", 
      g.group_id AS "g.group_id", 
      g.name AS "g.name", 
      r.member_id AS "r.from_id", 
      r.group_id AS "r.to_id", 
      u.department AS "u.department", 
      u.email AS "u.email", 
      u.exposure AS "u.exposure", 
      u.name AS "u.name", 
      u.user_id AS "u.user_id"
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_memberships AS r ON r.member_id = u.user_id AND r.member_type = 'User'
INNER JOIN data_security.ds_groups AS g ON g.group_id = r.group_id
WHERE u.name = 'Alice'
