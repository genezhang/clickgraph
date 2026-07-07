WITH vlp_multi_type_u_target AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, string(r1.from_id) AS r_from_id, string(r1.to_id) AS r_to_id, to_json(struct(p2.content AS content, p2.created_at AS created, p2.post_id AS post_id, p2.content AS title)) AS end_properties, to_json(struct(u_1.email_address, u_1.full_name, u_1.user_id)) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, array('FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight))) AS rel_properties, array(string(u_1.user_id), string(p2.post_id)) AS path_nodes
FROM brahmand.users_bench u_1
INNER JOIN brahmand.interactions r1 ON u_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND (u_1.user_id = 1)
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, string(r1.from_id) AS r_from_id, string(r1.to_id) AS r_to_id, to_json(struct(u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id)) AS end_properties, to_json(struct(u_1.email_address, u_1.full_name, u_1.user_id)) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, array('FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight))) AS rel_properties, array(string(u_1.user_id), string(u2.user_id)) AS path_nodes
FROM brahmand.users_bench u_1
INNER JOIN brahmand.interactions r1 ON u_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND (u_1.user_id = 1)
UNION ALL
SELECT 'Post' AS end_type, p2.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, string(r1.from_id) AS r_from_id, string(r1.to_id) AS r_to_id, to_json(struct(p2.content AS content, p2.created_at AS created, p2.post_id AS post_id, p2.content AS title)) AS end_properties, to_json(struct(u_1.email_address, u_1.full_name, u_1.user_id)) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, array('LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight))) AS rel_properties, array(string(u_1.user_id), string(p2.post_id)) AS path_nodes
FROM brahmand.users_bench u_1
INNER JOIN brahmand.interactions r1 ON u_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND (u_1.user_id = 1)
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, string(r1.from_id) AS r_from_id, string(r1.to_id) AS r_to_id, to_json(struct(u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id)) AS end_properties, to_json(struct(u_1.email_address, u_1.full_name, u_1.user_id)) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, array('LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight))) AS rel_properties, array(string(u_1.user_id), string(u2.user_id)) AS path_nodes
FROM brahmand.users_bench u_1
INNER JOIN brahmand.interactions r1 ON u_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND (u_1.user_id = 1)
)
SELECT 
      t.end_properties AS `target.properties`, 
      t.end_id AS `target.id`, 
      t.end_type AS `target.__label__`
FROM vlp_multi_type_u_target AS t
WHERE t.start_id = 1
LIMIT 10