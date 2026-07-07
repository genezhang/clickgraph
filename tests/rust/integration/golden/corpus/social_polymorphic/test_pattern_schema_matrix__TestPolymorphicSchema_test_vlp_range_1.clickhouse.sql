WITH vlp_multi_type_a_b AS (
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id) AS end_properties, u2.email_address AS end_email, u2.full_name AS end_name, u2.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 1 AS hop_count, ['AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['AUTHORED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'AUTHORED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id) AS end_properties, u2.email_address AS end_email, u2.full_name AS end_name, u2.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 1 AS hop_count, ['COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['COMMENTED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'COMMENTED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id) AS end_properties, u2.email_address AS end_email, u2.full_name AS end_name, u2.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 1 AS hop_count, ['FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['FOLLOWS', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id) AS end_properties, u2.email_address AS end_email, u2.full_name AS end_name, u2.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 1 AS hop_count, ['LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['LIKES', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
INNER JOIN brahmand.interactions r2 ON p2.post_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'Post' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'Post' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id) AS end_properties, u2.email_address AS end_email, u2.full_name AS end_name, u2.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 1 AS hop_count, ['SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'AUTHORED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'AUTHORED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'COMMENTED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'COMMENTED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'FOLLOWS', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'FOLLOWS' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'LIKES', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'LIKES' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(p3.post_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.posts_bench p3 ON r2.to_id = p3.post_id
INNER JOIN brahmand.interactions r3 ON p3.post_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'Post' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'Post' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.email_address AS email, u3.full_name AS name, u3.user_id AS user_id) AS end_properties, u3.email_address AS end_email, u3.full_name AS end_name, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 2 AS hop_count, ['SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'AUTHORED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'COMMENTED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'COMMENTED' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'FOLLOWS' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'LIKES' AND r3.from_type = 'User' AND r3.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u4.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u4.email_address AS email, u4.full_name AS name, u4.user_id AS user_id) AS end_properties, u4.email_address AS end_email, u4.full_name AS end_name, u4.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', a_1.email_address, a_1.full_name, a_1.user_id) AS start_properties, a_1.email_address AS start_email, a_1.full_name AS start_name, a_1.user_id AS start_user_id, 3 AS hop_count, ['SHARED', 'SHARED', 'SHARED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight), formatRowNoNewline('JSONEachRow', r2.timestamp, r2.interaction_weight), formatRowNoNewline('JSONEachRow', r3.timestamp, r3.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id), toString(u3.user_id), toString(u4.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
INNER JOIN brahmand.interactions r2 ON u2.user_id = r2.from_id
INNER JOIN brahmand.users_bench u3 ON r2.to_id = u3.user_id
INNER JOIN brahmand.interactions r3 ON u3.user_id = r3.from_id
INNER JOIN brahmand.users_bench u4 ON r3.to_id = u4.user_id
WHERE r1.interaction_type = 'SHARED' AND r1.from_type = 'User' AND r1.to_type = 'User' AND r2.interaction_type = 'SHARED' AND r2.from_type = 'User' AND r2.to_type = 'User' AND r3.interaction_type = 'SHARED' AND r3.from_type = 'User' AND r3.to_type = 'User'
)
SELECT 
      JSONExtractString(t.start_properties, 'full_name') AS "a.name", 
      JSONExtractString(t.end_properties, 'full_name') AS "b.name"
FROM vlp_multi_type_a_b AS t
LIMIT 10