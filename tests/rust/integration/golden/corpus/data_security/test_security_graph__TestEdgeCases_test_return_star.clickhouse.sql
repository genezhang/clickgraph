SELECT 
      v2.description AS "g.description", 
      v2.group_id AS "g.group_id", 
      v2.name AS "g.name", 
      v1.member_id AS "r.from_id", 
      v1.group_id AS "r.to_id", 
      v0.department AS "u.department", 
      v0.email AS "u.email", 
      v0.exposure AS "u.exposure", 
      v0.name AS "u.name", 
      v0.user_id AS "u.user_id"
FROM data_security.ds_users AS v0
JOIN data_security.ds_memberships AS v1 ON v1.member_id = v0.user_id
JOIN data_security.ds_groups AS v2 ON v1.group_id = v2.group_id
WHERE (((v1."member_type" = 'User')) AND v0.name = 'Alice')
