WITH with_w1 AS (
SELECT 
      v0.group_id AS `v3__group_id`, 
      v0.name AS `p2_v3_name`, 
      count(v2.user_id) AS `v4`
FROM data_security.ds_groups AS v0
JOIN data_security.ds_memberships AS v1 ON v1.group_id = v0.group_id
JOIN data_security.ds_users AS v2 ON v1.member_id = v2.user_id
WHERE ((v1.`member_type` = 'User'))
GROUP BY v0.group_id, v0.name
HAVING count(v2.user_id) >= 2
)
SELECT 
      w1.p2_v3_name AS `g.name`, 
      w1.v4 AS `cnt`
FROM with_w1 AS w1
