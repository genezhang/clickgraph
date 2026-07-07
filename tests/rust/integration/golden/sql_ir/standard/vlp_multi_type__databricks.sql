WITH vlp_multi_type_a_b AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(p2.author_id AS author_id, p2.post_content AS content, p2.post_date AS date, p2.post_id AS post_id, p2.post_title AS title)) AS end_properties, p2.author_id AS end_author_id, p2.post_content AS end_content, p2.post_date AS end_date, p2.post_id AS end_post_id, p2.post_title AS end_title, 1 AS hop_count, array('AUTHORED') AS path_relationships, array(to_json(struct(r1.authored_date))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id)) AS path_nodes
FROM social.users_bench a_1
INNER JOIN social.authored_bench r1 ON a_1.user_id = r1.user_id
INNER JOIN social.posts_bench p2 ON r1.post_id = p2.post_id
UNION ALL
SELECT 'Post' AS end_type, p3.post_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(p3.author_id AS author_id, p3.post_content AS content, p3.post_date AS date, p3.post_id AS post_id, p3.post_title AS title)) AS end_properties, p3.author_id AS end_author_id, p3.post_content AS end_content, p3.post_date AS end_date, p3.post_id AS end_post_id, p3.post_title AS end_title, 2 AS hop_count, array('FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.follow_date)), to_json(struct(r2.authored_date))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(p3.post_id)) AS path_nodes
FROM social.users_bench a_1
INNER JOIN social.user_follows_bench r1 ON a_1.user_id = r1.follower_id
INNER JOIN social.users_bench u2 ON r1.followed_id = u2.user_id
INNER JOIN social.authored_bench r2 ON u2.user_id = r2.user_id
INNER JOIN social.posts_bench p3 ON r2.post_id = p3.post_id
), 
vlp_multi_type_a_b_2 AS (
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u2.city AS city, u2.country AS country, u2.email_address AS email, u2.is_active AS is_active, u2.full_name AS name, u2.registration_date AS registration_date, u2.user_id AS user_id)) AS end_properties, u2.city AS end_city, u2.country AS end_country, u2.email_address AS end_email, u2.is_active AS end_is_active, u2.full_name AS end_name, u2.registration_date AS end_registration_date, u2.user_id AS end_user_id, 1 AS hop_count, array('FOLLOWS') AS path_relationships, array(to_json(struct(r1.follow_date))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id)) AS path_nodes
FROM social.users_bench a_1
INNER JOIN social.user_follows_bench r1 ON a_1.user_id = r1.follower_id
INNER JOIN social.users_bench u2 ON r1.followed_id = u2.user_id
UNION ALL
SELECT 'User' AS end_type, u3.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u3.city AS city, u3.country AS country, u3.email_address AS email, u3.is_active AS is_active, u3.full_name AS name, u3.registration_date AS registration_date, u3.user_id AS user_id)) AS end_properties, u3.city AS end_city, u3.country AS end_country, u3.email_address AS end_email, u3.is_active AS end_is_active, u3.full_name AS end_name, u3.registration_date AS end_registration_date, u3.user_id AS end_user_id, 2 AS hop_count, array('FOLLOWS', 'FOLLOWS') AS path_relationships, array(to_json(struct(r1.follow_date)), to_json(struct(r2.follow_date))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id), string(u3.user_id)) AS path_nodes
FROM social.users_bench a_1
INNER JOIN social.user_follows_bench r1 ON a_1.user_id = r1.follower_id
INNER JOIN social.users_bench u2 ON r1.followed_id = u2.user_id
INNER JOIN social.user_follows_bench r2 ON u2.user_id = r2.follower_id
INNER JOIN social.users_bench u3 ON r2.followed_id = u3.user_id
)
SELECT 
      t.end_properties AS `b.properties`, 
      t.end_id AS `b.id`, 
      t.end_type AS `b.__label__`
FROM vlp_multi_type_a_b AS t
UNION ALL 
SELECT 
      t.end_properties AS `b.properties`, 
      t.end_id AS `b.id`, 
      t.end_type AS `b.__label__`
FROM vlp_multi_type_a_b_2 AS t
