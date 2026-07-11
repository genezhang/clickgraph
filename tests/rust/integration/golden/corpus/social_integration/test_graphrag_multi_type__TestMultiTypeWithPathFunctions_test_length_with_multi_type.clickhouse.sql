WITH vlp_multi_type_u_x AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', p2.author_id AS author_id, p2.post_content AS content, p2.post_date AS created_at, p2.post_id AS post_id, p2.post_title AS title) AS end_properties, p2.author_id AS end_author_id, p2.post_content AS end_content, p2.post_date AS end_created_at, p2.post_id AS end_post_id, p2.post_title AS end_title, formatRowNoNewline('JSONEachRow', u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id) AS start_properties, u_1.age AS start_age, u_1.city AS start_city, u_1.country AS start_country, u_1.email_address AS start_email, u_1.is_active AS start_is_active, u_1.full_name AS start_name, u_1.registration_date AS start_registration_date, u_1.user_id AS start_user_id, 1 AS hop_count, ['AUTHORED'] AS path_relationships, ['{}'] AS rel_properties, [toString(u_1.user_id), toString(p2.post_id)] AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.posts_test p2 ON u_1.user_id = p2.author_id
WHERE (u_1.user_id = 1)
UNION ALL
SELECT 'Post' AS end_type, p3.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, formatRowNoNewline('JSONEachRow', p3.author_id AS author_id, p3.post_content AS content, p3.post_date AS created_at, p3.post_id AS post_id, p3.post_title AS title) AS end_properties, p3.author_id AS end_author_id, p3.post_content AS end_content, p3.post_date AS end_created_at, p3.post_id AS end_post_id, p3.post_title AS end_title, formatRowNoNewline('JSONEachRow', u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id) AS start_properties, u_1.age AS start_age, u_1.city AS start_city, u_1.country AS start_country, u_1.email_address AS start_email, u_1.is_active AS start_is_active, u_1.full_name AS start_name, u_1.registration_date AS start_registration_date, u_1.user_id AS start_user_id, 2 AS hop_count, ['FOLLOWS', 'AUTHORED'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.follow_date), '{}'] AS rel_properties, [toString(u_1.user_id), toString(u2.user_id), toString(p3.post_id)] AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.user_follows_test r1 ON u_1.user_id = r1.follower_id
INNER JOIN test_integration.users_test u2 ON r1.followed_id = u2.user_id
INNER JOIN test_integration.posts_test p3 ON u2.user_id = p3.author_id
WHERE (u_1.user_id = 1)
)
SELECT `path_length` AS "path_length", count(*) AS "cnt" FROM (
SELECT 
      t.hop_count AS "path_length",
      t.hop_count AS "t.hop_count"
FROM vlp_multi_type_u_x AS t
UNION ALL 
SELECT 
      tuple('fixed_path', 'u', 'x', 't0') AS "p",
      formatRowNoNewline('JSONEachRow', u.age AS _s_age, u.city AS _s_city, u.country AS _s_country, u.email_address AS _s_email, u.is_active AS _s_is_active, u.full_name AS _s_name, u.registration_date AS _s_registration_date, u.user_id AS _s_user_id) AS "_start_properties",
      formatRowNoNewline('JSONEachRow', x.age AS _e_age, x.city AS _e_city, x.country AS _e_country, x.email_address AS _e_email, x.is_active AS _e_is_active, x.full_name AS _e_name, x.registration_date AS _e_registration_date, x.user_id AS _e_user_id) AS "_end_properties",
      '{}' AS "_rel_properties",
      'FOLLOWS' AS "__rel_type__",
      'User' AS "__start_label__",
      'User' AS "__end_label__",
      u.age AS "age",
      u.city AS "city",
      u.country AS "country",
      u.email_address AS "email",
      u.is_active AS "is_active",
      u.full_name AS "name",
      u.registration_date AS "registration_date",
      u.user_id AS "user_id",
      x.age AS "age_2",
      x.city AS "city_2",
      x.country AS "country_2",
      x.email_address AS "email_2",
      x.is_active AS "is_active_2",
      x.full_name AS "name_2",
      x.registration_date AS "registration_date_2",
      x.user_id AS "user_id_2",
      t.hop_count AS "path_length",
      t.hop_count AS "t.hop_count"
FROM vlp_multi_type_u_x_2 AS t
) AS __union
GROUP BY `path_length`
ORDER BY `path_length` ASC
