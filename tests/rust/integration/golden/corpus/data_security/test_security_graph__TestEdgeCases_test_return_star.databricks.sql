SELECT 
      v2.description AS `g.description`, 
      v2.group_id AS `g.group_id`, 
      v2.name AS `g.name`, 
      v1.member_id AS `r.from_id`, 
      v1.group_id AS `r.to_id`, 
      v1.member_id AS `r.edge_id_1`, 
      v1.group_id AS `r.edge_id_2`, 
      v0.department AS `u.department`, 
      v0.email AS `u.email`, 
      v0.exposure AS `u.exposure`, 
      v0.name AS `u.name`, 
      v0.user_id AS `u.user_id`
FROM data_security.ds_users AS v0
JOIN data_security.ds_memberships AS v1 ON v1.member_id = v0.user_id
JOIN data_security.ds_groups AS v2 ON v1.group_id = v2.group_id
WHERE (((v1.`member_type` = 'User')) AND v0.name = 'Alice')

-- column g: Node { label: "Group" } [("description", "g.description"), ("group_id", "g.group_id"), ("name", "g.name")]
-- column r: Rel { rel_type: "MEMBER_OF", from_label: "User", to_label: "Group" } [("from_id", "r.from_id"), ("to_id", "r.to_id"), ("edge_id_1", "r.edge_id_1"), ("edge_id_2", "r.edge_id_2")]
-- column u: Node { label: "User" } [("department", "u.department"), ("email", "u.email"), ("exposure", "u.exposure"), ("name", "u.name"), ("user_id", "u.user_id")]