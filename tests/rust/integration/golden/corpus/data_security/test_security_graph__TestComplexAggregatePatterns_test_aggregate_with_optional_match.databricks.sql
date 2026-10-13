WITH optional_o1 AS (
SELECT 
      v1.group_id AS `v0__group_id`, 
      v2.user_id AS `v2__user_id`
FROM data_security.ds_memberships AS v1
JOIN data_security.ds_users AS v2 ON v1.member_id = v2.user_id
WHERE ((v1.`member_type` = 'User'))
)
SELECT 
      v0.name AS `g.name`, 
      count(o1.v2__user_id) AS `member_count`
FROM data_security.ds_groups AS v0
LEFT JOIN optional_o1 AS o1 ON v0.group_id = o1.v0__group_id
GROUP BY v0.name
ORDER BY v0.name ASC NULLS LAST
