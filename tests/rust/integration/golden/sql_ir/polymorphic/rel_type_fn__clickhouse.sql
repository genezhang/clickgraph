WITH vlp_multi_type_a_b AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, toString(r1.from_id) AS r_from_id, toString(r1.to_id) AS r_to_id, formatRowNoNewline('JSONEachRow', p2.content AS content, p2.created_at AS created, p2.post_id AS post_id, p2.content AS title) AS end_properties, 1 AS hop_count, ['FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'Post'
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, toString(r1.from_id) AS r_from_id, toString(r1.to_id) AS r_to_id, formatRowNoNewline('JSONEachRow', u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id) AS end_properties, 1 AS hop_count, ['FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User'
UNION ALL
SELECT 'Post' AS end_type, p2.post_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, toString(r1.from_id) AS r_from_id, toString(r1.to_id) AS r_to_id, formatRowNoNewline('JSONEachRow', p2.content AS content, p2.created_at AS created, p2.post_id AS post_id, p2.content AS title) AS end_properties, 1 AS hop_count, ['LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.posts_bench p2 ON r1.to_id = p2.post_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'Post'
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, toString(r1.from_id) AS r_from_id, toString(r1.to_id) AS r_to_id, formatRowNoNewline('JSONEachRow', u2.email_address AS email, u2.full_name AS name, u2.user_id AS user_id) AS end_properties, 1 AS hop_count, ['LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User'
)
SELECT 
      t.path_relationships[1] AS "t"
FROM vlp_multi_type_a_b AS t
