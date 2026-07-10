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
)
SELECT `node_type` AS "node_type", count(*) AS "cnt" FROM (
SELECT DISTINCT 
      JSONExtractString(t.end_properties, 'end_type') AS "node_type"
FROM vlp_multi_type_u_x AS t
UNION ALL 
SELECT 
      toString(t.age) AS "age",
      NULL AS "author_id",
      toString(t.city) AS "city",
      NULL AS "content",
      toString(t.country) AS "country",
      NULL AS "created_at",
      toString(t.email_address) AS "email",
      toString(t.is_active) AS "is_active",
      toString(t.full_name) AS "name",
      NULL AS "post_id",
      toString(t.registration_date) AS "registration_date",
      NULL AS "title",
      toString(t.user_id) AS "user_id",
      JSONExtractString(t.end_properties, 'end_type') AS "node_type"
FROM vlp_multi_type_u_x_2 AS t
) AS __union
GROUP BY t.end_end_type
ORDER BY node_type ASC
