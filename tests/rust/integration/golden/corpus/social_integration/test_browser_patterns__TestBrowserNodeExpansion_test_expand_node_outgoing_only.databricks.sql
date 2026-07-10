WITH vlp_multi_type_n_m AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, n_1.user_id AS start_id, 'User' AS start_type, string(p2.author_id) AS r_from_id, string(p2.post_id) AS r_to_id, to_json(struct(p2.author_id AS author_id, p2.post_content AS content, p2.post_date AS created_at, p2.post_id AS post_id, p2.post_title AS title)) AS end_properties, to_json(struct(n_1.age, n_1.city, n_1.country, n_1.email_address, n_1.is_active, n_1.full_name, n_1.registration_date, n_1.user_id)) AS start_properties, n_1.user_id AS start_user_id, 1 AS hop_count, array('AUTHORED') AS path_relationships, array('{}') AS rel_properties, array(string(n_1.user_id), string(p2.post_id)) AS path_nodes
FROM test_integration.users_test n_1
INNER JOIN test_integration.posts_test p2 ON n_1.user_id = p2.author_id
WHERE (n_1.user_id = 1)
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, n_1.user_id AS start_id, 'User' AS start_type, string(r1.follower_id) AS r_from_id, string(r1.followed_id) AS r_to_id, to_json(struct(u2.age AS age, u2.city AS city, u2.country AS country, u2.email_address AS email, u2.is_active AS is_active, u2.full_name AS name, u2.registration_date AS registration_date, u2.user_id AS user_id)) AS end_properties, to_json(struct(n_1.age, n_1.city, n_1.country, n_1.email_address, n_1.is_active, n_1.full_name, n_1.registration_date, n_1.user_id)) AS start_properties, n_1.user_id AS start_user_id, 1 AS hop_count, array('FOLLOWS') AS path_relationships, array(to_json(struct(r1.follow_date))) AS rel_properties, array(string(n_1.user_id), string(u2.user_id)) AS path_nodes
FROM test_integration.users_test n_1
INNER JOIN test_integration.user_follows_test r1 ON n_1.user_id = r1.follower_id
INNER JOIN test_integration.users_test u2 ON r1.followed_id = u2.user_id
WHERE (n_1.user_id = 1)
UNION ALL
SELECT 'Post' AS end_type, p2.post_id AS end_id, n_1.user_id AS start_id, 'User' AS start_type, string(r1.user_id) AS r_from_id, string(r1.post_id) AS r_to_id, to_json(struct(p2.author_id AS author_id, p2.post_content AS content, p2.post_date AS created_at, p2.post_id AS post_id, p2.post_title AS title)) AS end_properties, to_json(struct(n_1.age, n_1.city, n_1.country, n_1.email_address, n_1.is_active, n_1.full_name, n_1.registration_date, n_1.user_id)) AS start_properties, n_1.user_id AS start_user_id, 1 AS hop_count, array('LIKED') AS path_relationships, array(to_json(struct(r1.like_date))) AS rel_properties, array(string(n_1.user_id), string(p2.post_id)) AS path_nodes
FROM test_integration.users_test n_1
INNER JOIN test_integration.post_likes_test r1 ON n_1.user_id = r1.user_id
INNER JOIN test_integration.posts_test p2 ON r1.post_id = p2.post_id
WHERE (n_1.user_id = 1)
)
SELECT 
      count(*) AS `count`
FROM vlp_multi_type_n_m AS t
WHERE t.start_id = 1
