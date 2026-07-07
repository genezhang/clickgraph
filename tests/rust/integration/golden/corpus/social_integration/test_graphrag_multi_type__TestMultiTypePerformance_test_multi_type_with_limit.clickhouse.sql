WITH vlp_multi_type_u_x AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', p2.author_id AS author_id, p2.post_content AS content, p2.post_date AS created_at, p2.post_id AS post_id, p2.post_title AS title) AS end_properties, p2.author_id AS end_author_id, p2.post_content AS end_content, p2.post_date AS end_created_at, p2.post_id AS end_post_id, p2.post_title AS end_title, formatRowNoNewline('JSONEachRow', u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, ['AUTHORED'] AS path_relationships, ['{}'] AS rel_properties, [toString(u_1.user_id), toString(p2.post_id)] AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.posts_test p2 ON u_1.user_id = p2.author_id
WHERE (u_1.user_id = 1)
UNION ALL
SELECT 'Post' AS end_type, p3.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', p3.author_id AS author_id, p3.post_content AS content, p3.post_date AS created_at, p3.post_id AS post_id, p3.post_title AS title) AS end_properties, p3.author_id AS end_author_id, p3.post_content AS end_content, p3.post_date AS end_created_at, p3.post_id AS end_post_id, p3.post_title AS end_title, formatRowNoNewline('JSONEachRow', u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id) AS start_properties, u_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.follow_date), '{}'] AS rel_properties, [toString(u_1.user_id), toString(u2.user_id), toString(p3.post_id)] AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.user_follows_test r1 ON u_1.user_id = r1.follower_id
INNER JOIN test_integration.users_test u2 ON r1.followed_id = u2.user_id
INNER JOIN test_integration.posts_test p3 ON u2.user_id = p3.author_id
WHERE (u_1.user_id = 1)
), 
vlp_multi_type_u_x_2 AS (
SELECT 'User' AS end_type, u2.user_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u2.age AS age, u2.city AS city, u2.country AS country, u2.email_address AS email, u2.is_active AS is_active, u2.full_name AS name, u2.registration_date AS registration_date, u2.user_id AS user_id) AS end_properties, u2.age AS end_age, u2.city AS end_city, u2.country AS end_country, u2.email_address AS end_email, u2.is_active AS end_is_active, u2.full_name AS end_name, u2.registration_date AS end_registration_date, u2.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, ['FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.follow_date)] AS rel_properties, [toString(u_1.user_id), toString(u2.user_id)] AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.user_follows_test r1 ON u_1.user_id = r1.follower_id
INNER JOIN test_integration.users_test u2 ON r1.followed_id = u2.user_id
WHERE (u_1.user_id = 1)
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', u3.age AS age, u3.city AS city, u3.country AS country, u3.email_address AS email, u3.is_active AS is_active, u3.full_name AS name, u3.registration_date AS registration_date, u3.user_id AS user_id) AS end_properties, u3.age AS end_age, u3.city AS end_city, u3.country AS end_country, u3.email_address AS end_email, u3.is_active AS end_is_active, u3.full_name AS end_name, u3.registration_date AS end_registration_date, u3.user_id AS end_user_id, formatRowNoNewline('JSONEachRow', u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id) AS start_properties, u_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.follow_date), formatRowNoNewline('JSONEachRow', r2.follow_date)] AS rel_properties, [toString(u_1.user_id), toString(u2.user_id), toString(u3.user_id)] AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.user_follows_test r1 ON u_1.user_id = r1.follower_id
INNER JOIN test_integration.users_test u2 ON r1.followed_id = u2.user_id
INNER JOIN test_integration.user_follows_test r2 ON u2.user_id = r2.follower_id
INNER JOIN test_integration.users_test u3 ON r2.followed_id = u3.user_id
WHERE (u_1.user_id = 1)
)
SELECT `x.properties` AS `x.properties`, `x.id` AS `x.id`, `x.__label__` AS `x.__label__` FROM (
SELECT 
      t.end_properties AS "x.properties", 
      t.end_id AS "x.id", 
      t.end_type AS "x.__label__"
FROM vlp_multi_type_u_x AS t
UNION ALL 
SELECT 
      t.end_properties AS "x.properties", 
      t.end_id AS "x.id", 
      t.end_type AS "x.__label__"
FROM vlp_multi_type_u_x_2 AS t
) AS __union
LIMIT 10