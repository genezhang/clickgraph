SELECT 
      v0.name AS "g.name", 
      count(v2.user_id) AS "cnt"
FROM data_security.ds_groups AS v0
JOIN data_security.ds_memberships AS v1 ON v1.group_id = v0.group_id
JOIN data_security.ds_users AS v2 ON v1.member_id = v2.user_id
WHERE ((v1."member_type" = 'User'))
GROUP BY v0.name
ORDER BY v0.name ASC
LIMIT 1, 5