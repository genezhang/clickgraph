WITH vlp_multi_type_u_x AS (
SELECT 'Post' AS end_type, p3.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', p3.author_id AS author_id, p3.post_content AS content, p3.post_date AS created_at, p3.post_id AS post_id, p3.post_title AS title) AS end_properties, p3.author_id AS end_author_id, p3.post_content AS end_content, p3.post_date AS end_created_at, p3.post_id AS end_post_id, p3.post_title AS end_title, formatRowNoNewline('JSONEachRow', u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id) AS start_properties, u_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.follow_date), '{}'] AS rel_properties, [toString(u_1.user_id), toString(u2.user_id), toString(p3.post_id)] AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.user_follows_test r1 ON u_1.user_id = r1.follower_id
INNER JOIN test_integration.users_test u2 ON r1.followed_id = u2.user_id
INNER JOIN test_integration.posts_test p3 ON u2.user_id = p3.author_id
WHERE (u_1.user_id = 1)
), 
vlp_multi_type_u_x_2 AS (
SELECT 'User' AS end_type, u3.user_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.age AS age, u3.city AS city, u3.country AS country, u3.email_address AS email, u3.is_active AS is_active, u3.full_name AS name, u3.registration_date AS registration_date, u3.user_id AS user_id) AS end_properties, u3.age AS end_age, u3.city AS end_city, u3.country AS end_country, u3.email_address AS end_email, u3.is_active AS end_is_active, u3.full_name AS end_name, u3.registration_date AS end_registration_date, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id) AS start_properties, u_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.follow_date), formatRowNoNewline('JSONEachRow', r2.follow_date)] AS rel_properties, [toString(u_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.user_follows_test r1 ON u_1.user_id = r1.follower_id
INNER JOIN test_integration.users_test u2 ON r1.followed_id = u2.user_id
INNER JOIN test_integration.user_follows_test r2 ON u2.user_id = r2.follower_id
INNER JOIN test_integration.users_test u3 ON r2.followed_id = u3.user_id
WHERE (u_1.user_id = 1)
)
SELECT `node_type` AS "node_type", count(*) AS "cnt" FROM (
SELECT DISTINCT 
      JSONExtractString(t.end_properties, 'end_type') AS "node_type"
FROM vlp_multi_type_u_x AS t
INNER JOIN test_integration.vlp_multi_type_u_x AS t0 ON t0.from_node_id = t.start_id
WHERE t.start_id = 1
UNION ALL 
SELECT DISTINCT 
      JSONExtractString(t.end_properties, 'end_type') AS "node_type"
FROM vlp_multi_type_u_x_2 AS t
INNER JOIN test_integration.vlp_multi_type_u_x AS t0 ON t0.from_node_id = t.start_id
WHERE t.start_id = 1
) AS __union
GROUP BY t.end_end_type
ORDER BY `node_type` ASC
