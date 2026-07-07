WITH vlp_multi_type_a_o AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, toString(r1.user_id) AS r_from_id, toString(r1.post_id) AS r_to_id, formatRowNoNewline('JSONEachRow', p2.author_id AS author_id, p2.post_content AS content, p2.post_date AS date, p2.post_id AS post_id, p2.post_title AS title) AS end_properties, formatRowNoNewline('JSONEachRow', a_1.city, a_1.country, a_1.email_address, a_1.is_active, a_1.full_name, a_1.registration_date, a_1.user_id) AS start_properties, a_1.city AS start_city, a_1.country AS start_country, a_1.email_address AS start_email, a_1.is_active AS start_is_active, a_1.full_name AS start_name, a_1.registration_date AS start_registration_date, a_1.user_id AS start_user_id, 1 AS hop_count, ['AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.authored_date)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id)] AS path_nodes
FROM social.users_bench a_1
INNER JOIN social.authored_bench r1 ON a_1.user_id = r1.user_id
INNER JOIN social.posts_bench p2 ON r1.post_id = p2.post_id
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, toString(r1.follower_id) AS r_from_id, toString(r1.followed_id) AS r_to_id, formatRowNoNewline('JSONEachRow', u2.city AS city, u2.country AS country, u2.email_address AS email, u2.is_active AS is_active, u2.full_name AS name, u2.registration_date AS registration_date, u2.user_id AS user_id) AS end_properties, formatRowNoNewline('JSONEachRow', a_1.city, a_1.country, a_1.email_address, a_1.is_active, a_1.full_name, a_1.registration_date, a_1.user_id) AS start_properties, a_1.city AS start_city, a_1.country AS start_country, a_1.email_address AS start_email, a_1.is_active AS start_is_active, a_1.full_name AS start_name, a_1.registration_date AS start_registration_date, a_1.user_id AS start_user_id, 1 AS hop_count, ['FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.follow_date)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM social.users_bench a_1
INNER JOIN social.user_follows_bench r1 ON a_1.user_id = r1.follower_id
INNER JOIN social.users_bench u2 ON r1.followed_id = u2.user_id
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, toString(r1.follower_id) AS r_from_id, toString(r1.followed_id) AS r_to_id, formatRowNoNewline('JSONEachRow', u2.city AS city, u2.country AS country, u2.email_address AS email, u2.is_active AS is_active, u2.full_name AS name, u2.registration_date AS registration_date, u2.user_id AS user_id) AS end_properties, formatRowNoNewline('JSONEachRow', a_1.city, a_1.country, a_1.email_address, a_1.is_active, a_1.full_name, a_1.registration_date, a_1.user_id) AS start_properties, a_1.city AS start_city, a_1.country AS start_country, a_1.email_address AS start_email, a_1.is_active AS start_is_active, a_1.full_name AS start_name, a_1.registration_date AS start_registration_date, a_1.user_id AS start_user_id, 1 AS hop_count, ['FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.follow_date)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM social.users_bench a_1
INNER JOIN social.user_follows_bench r1 ON a_1.user_id = r1.followed_id
INNER JOIN social.users_bench u2 ON r1.follower_id = u2.user_id
UNION ALL
SELECT 'Post' AS end_type, p2.post_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, toString(r1.user_id) AS r_from_id, toString(r1.post_id) AS r_to_id, formatRowNoNewline('JSONEachRow', p2.author_id AS author_id, p2.post_content AS content, p2.post_date AS date, p2.post_id AS post_id, p2.post_title AS title) AS end_properties, formatRowNoNewline('JSONEachRow', a_1.city, a_1.country, a_1.email_address, a_1.is_active, a_1.full_name, a_1.registration_date, a_1.user_id) AS start_properties, a_1.city AS start_city, a_1.country AS start_country, a_1.email_address AS start_email, a_1.is_active AS start_is_active, a_1.full_name AS start_name, a_1.registration_date AS start_registration_date, a_1.user_id AS start_user_id, 1 AS hop_count, ['LIKED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.like_date)] AS rel_properties, [toString(a_1.user_id), toString(p2.post_id)] AS path_nodes
FROM social.users_bench a_1
INNER JOIN social.post_likes_bench r1 ON a_1.user_id = r1.user_id
INNER JOIN social.posts_bench p2 ON r1.post_id = p2.post_id
)
SELECT 
      t.start_properties AS "a.properties", 
      t.start_id AS "a.id", 
      t.start_type AS "a.__label__", 
      t.path_relationships AS "r.type", 
      t.rel_properties AS "r.properties", 
      t.start_id AS "r.start_id", 
      t.end_id AS "r.end_id", 
      t.end_properties AS "o.properties", 
      t.end_id AS "o.id", 
      t.end_type AS "o.__label__", 
      t.r_from_id AS "r.r_from_id", 
      t.r_to_id AS "r.r_to_id"
FROM vlp_multi_type_a_o AS t
