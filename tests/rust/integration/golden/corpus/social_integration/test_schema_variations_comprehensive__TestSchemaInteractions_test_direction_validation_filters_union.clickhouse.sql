WITH vlp_multi_type_p_u AS (
SELECT 'User' AS end_type, u2.user_id AS end_id, p_1.post_id AS start_id, 'Post' AS start_type, toString(r1.author_id) AS r_from_id, toString(r1.post_id) AS r_to_id, formatRowNoNewline('JSONEachRow', u2.age AS age, u2.city AS city, u2.country AS country, u2.email_address AS email, u2.is_active AS is_active, u2.full_name AS name, u2.registration_date AS registration_date, u2.user_id AS user_id) AS end_properties, u2.age AS end_age, u2.city AS end_city, u2.country AS end_country, u2.email_address AS end_email, u2.is_active AS end_is_active, u2.full_name AS end_name, u2.registration_date AS end_registration_date, u2.user_id AS end_user_id, 1 AS hop_count, ['AUTHORED'] AS path_relationships, ['{}'] AS rel_properties, [toString(p_1.post_id), toString(u2.user_id)] AS path_nodes
FROM test_integration.posts_test p_1
INNER JOIN test_integration.posts_test r1 ON p_1.post_id = r1.post_id
INNER JOIN test_integration.users_test u2 ON r1.author_id = u2.user_id
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, p_1.post_id AS start_id, 'Post' AS start_type, toString(r1.user_id) AS r_from_id, toString(r1.post_id) AS r_to_id, formatRowNoNewline('JSONEachRow', u2.age AS age, u2.city AS city, u2.country AS country, u2.email_address AS email, u2.is_active AS is_active, u2.full_name AS name, u2.registration_date AS registration_date, u2.user_id AS user_id) AS end_properties, u2.age AS end_age, u2.city AS end_city, u2.country AS end_country, u2.email_address AS end_email, u2.is_active AS end_is_active, u2.full_name AS end_name, u2.registration_date AS end_registration_date, u2.user_id AS end_user_id, 1 AS hop_count, ['LIKED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.like_date)] AS rel_properties, [toString(p_1.post_id), toString(u2.user_id)] AS path_nodes
FROM test_integration.posts_test p_1
INNER JOIN test_integration.post_likes_test r1 ON p_1.post_id = r1.post_id
INNER JOIN test_integration.users_test u2 ON r1.user_id = u2.user_id
)
SELECT 
      count(DISTINCT tuple(t.end_id, t.end_id)) AS "total"
FROM vlp_multi_type_p_u AS t
