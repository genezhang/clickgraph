WITH vlp_multi_type_a_b AS (
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id)) AS end_properties, u2.email_address AS end_email, u2.full_name AS end_name, u2.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 1 AS hop_count, array('AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND (a_1.user_id != u2.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('AUTHORED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id)) AS end_properties, u2.email_address AS end_email, u2.full_name AS end_name, u2.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 1 AS hop_count, array('COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND (a_1.user_id != u2.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('COMMENTED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id)) AS end_properties, u2.email_address AS end_email, u2.full_name AS end_name, u2.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 1 AS hop_count, array('FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND (a_1.user_id != u2.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('FOLLOWS', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id)) AS end_properties, u2.email_address AS end_email, u2.full_name AS end_name, u2.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 1 AS hop_count, array('LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND (a_1.user_id != u2.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('LIKES', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id)) AS end_properties, u2.email_address AS end_email, u2.full_name AS end_name, u2.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 1 AS hop_count, array('SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND (a_1.user_id != u2.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'AUTHORED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'COMMENTED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'FOLLOWS', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'LIKES', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id)) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, array('SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND (a_1.user_id != u3.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'COMMENTED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'LIKES') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id)) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, to_json(struct(a_1.email_address, a_1.full_name, a_1.user_id)) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, array('SHARED', 'SHARED', 'SHARED') AS path_relationships, array(to_json(struct(r1.timestamp, r1.interaction_weight)), to_json(struct(r2.timestamp, r2.interaction_weight)), to_json(struct(r3.timestamp, r3.interaction_weight))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id), string(u4.user_id)) AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User' AND (a_1.user_id != u4.user_id)
)
SELECT 
      t.hop_count AS `length(p)`
FROM vlp_multi_type_a_b AS t
LIMIT 5