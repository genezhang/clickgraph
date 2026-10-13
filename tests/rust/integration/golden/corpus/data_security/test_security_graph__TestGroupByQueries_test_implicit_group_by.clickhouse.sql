SELECT 
      v2.name AS "g.name", 
      count(v0.user_id) AS "COUNT(u)"
FROM data_security.ds_users AS v0
JOIN data_security.ds_memberships AS v1 ON v1.member_id = v0.user_id
JOIN data_security.ds_groups AS v2 ON v1.group_id = v2.group_id
WHERE ((v1."member_type" = 'User'))
GROUP BY v2.name
ORDER BY v2.name ASC
