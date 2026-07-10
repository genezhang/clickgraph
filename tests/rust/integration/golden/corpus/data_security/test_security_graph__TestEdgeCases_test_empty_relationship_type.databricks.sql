WITH vlp_multi_type_u_g AS (
SELECT 'Group' AS end_type, n2.group_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, string(r1.subject_id) AS r_from_id, string(r1.object_id) AS r_to_id, to_json(struct(n2.description, n2.group_id, n2.name)) AS end_properties, n2.name AS end_name, to_json(struct(u_1.department, u_1.email, u_1.exposure, u_1.name, u_1.user_id)) AS start_properties, u_1.name AS start_name, 1 AS hop_count, array('HAS_ACCESS') AS path_relationships, array(to_json(struct(r1.privilege))) AS rel_properties, array(string(u_1.user_id), string(n2.group_id)) AS path_nodes
FROM data_security.ds_users u_1
INNER JOIN data_security.ds_permissions r1 ON u_1.user_id = r1.subject_id
INNER JOIN data_security.ds_groups n2 ON r1.object_id = n2.group_id
WHERE r1.subject_type = 'User' AND r1.object_type = 'Group'
UNION ALL
SELECT 'Group' AS end_type, n2.group_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, string(r1.member_id) AS r_from_id, string(r1.group_id) AS r_to_id, to_json(struct(n2.description, n2.group_id, n2.name)) AS end_properties, n2.name AS end_name, to_json(struct(u_1.department, u_1.email, u_1.exposure, u_1.name, u_1.user_id)) AS start_properties, u_1.name AS start_name, 1 AS hop_count, array('MEMBER_OF') AS path_relationships, array('{}') AS rel_properties, array(string(u_1.user_id), string(n2.group_id)) AS path_nodes
FROM data_security.ds_users u_1
INNER JOIN data_security.ds_memberships r1 ON u_1.user_id = r1.member_id
INNER JOIN data_security.ds_groups n2 ON r1.group_id = n2.group_id
WHERE r1.member_type = 'User'
)
SELECT 
      get_json_object(t.start_properties, '$.name') AS `u.name`, 
      element_at(t.path_relationships, 1) AS `type(r)`, 
      get_json_object(t.end_properties, '$.name') AS `g.name`
FROM vlp_multi_type_u_g AS t
LIMIT 5