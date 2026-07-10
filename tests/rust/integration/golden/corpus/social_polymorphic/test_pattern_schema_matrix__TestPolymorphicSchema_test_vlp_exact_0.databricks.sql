WITH vlp_multi_type_a_b AS (
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User'
)
SELECT 
      get_json_object(t.start_properties, '$.email_address') AS `a.email`, 
      get_json_object(t.end_properties, '$.email_address') AS `b.email`
FROM vlp_multi_type_a_b AS t
INNER JOIN brahmand.interactions AS r1 ON t.start_id = r1.from_id
WHERE t.start_id <> t.end_id
LIMIT 10