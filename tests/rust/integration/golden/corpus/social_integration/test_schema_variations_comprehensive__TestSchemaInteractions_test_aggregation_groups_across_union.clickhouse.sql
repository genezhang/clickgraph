WITH vlp_multi_type_u_target AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, toString(p2.author_id) AS r_from_id, toString(p2.post_id) AS r_to_id, formatRowNoNewline('JSONEachRow', p2.author_id AS author_id, p2.post_content AS content, p2.post_date AS created_at, p2.post_id AS post_id, p2.post_title AS title) AS end_properties, formatRowNoNewline('JSONEachRow', u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, ['AUTHORED'] AS path_relationships, ['{}'] AS rel_properties, [toString(u_1.user_id), toString(p2.post_id)] AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.posts_test p2 ON u_1.user_id = p2.author_id
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, toString(r1.follower_id) AS r_from_id, toString(r1.followed_id) AS r_to_id, formatRowNoNewline('JSONEachRow', u2.age AS age, u2.city AS city, u2.country AS country, u2.email_address AS email, u2.is_active AS is_active, u2.full_name AS name, u2.registration_date AS registration_date, u2.user_id AS user_id) AS end_properties, formatRowNoNewline('JSONEachRow', u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, ['FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.follow_date)] AS rel_properties, [toString(u_1.user_id), toString(u2.user_id)] AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.user_follows_test r1 ON u_1.user_id = r1.follower_id
INNER JOIN test_integration.users_test u2 ON r1.followed_id = u2.user_id
UNION ALL
SELECT 'Post' AS end_type, p2.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, toString(r1.user_id) AS r_from_id, toString(r1.post_id) AS r_to_id, formatRowNoNewline('JSONEachRow', p2.author_id AS author_id, p2.post_content AS content, p2.post_date AS created_at, p2.post_id AS post_id, p2.post_title AS title) AS end_properties, formatRowNoNewline('JSONEachRow', u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, ['LIKED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.like_date)] AS rel_properties, [toString(u_1.user_id), toString(p2.post_id)] AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.post_likes_test r1 ON u_1.user_id = r1.user_id
INNER JOIN test_integration.posts_test p2 ON r1.post_id = p2.post_id
)
SELECT 
      JSONExtractString(t.start_properties, 'user_id') AS "u.user_id", 
      t.path_relationships[1] AS "type(r)", 
      count(*) AS "action_count"
FROM vlp_multi_type_u_target AS t
GROUP BY t.start_id, t.path_relationships[1]
