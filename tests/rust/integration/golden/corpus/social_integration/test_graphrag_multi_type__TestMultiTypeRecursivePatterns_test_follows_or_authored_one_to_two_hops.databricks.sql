WITH vlp_multi_type_u_x AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id)) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, array('AUTHORED') AS path_relationships, array('{}') AS rel_properties, array(string(u_1.user_id), string(p2.post_id)) AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.posts_test p2 ON u_1.user_id = p2.author_id
WHERE (u_1.user_id = 1)
UNION ALL
SELECT 'Post' AS end_type, p3.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, to_json(struct(u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id)) AS start_properties, u_1.user_id AS start_user_id, 2 AS hop_count, array('FOLLOWS', 'AUTHORED') AS path_relationships, array(to_json(struct(r1.follow_date)), '{}') AS rel_properties, array(string(u_1.user_id), string(u2.user_id), string(p3.post_id)) AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.user_follows_test r1 ON u_1.user_id = r1.follower_id
INNER JOIN test_integration.users_test u2 ON r1.followed_id = u2.user_id
INNER JOIN test_integration.posts_test p3 ON u2.user_id = p3.author_id
WHERE (u_1.user_id = 1)
)
SELECT `node_type` AS `node_type`, count(*) AS `cnt` FROM (
SELECT DISTINCT 
      t.end_type AS `node_type`
FROM vlp_multi_type_u_x AS t
UNION ALL 
SELECT 
      string(t.age) AS `age`,
      NULL AS `author_id`,
      string(t.city) AS `city`,
      NULL AS `content`,
      string(t.country) AS `country`,
      NULL AS `created_at`,
      string(t.email_address) AS `email`,
      string(t.is_active) AS `is_active`,
      string(t.full_name) AS `name`,
      NULL AS `post_id`,
      string(t.registration_date) AS `registration_date`,
      NULL AS `title`,
      string(t.user_id) AS `user_id`,
      t.end_type AS `node_type`
FROM vlp_multi_type_u_x_2 AS t
) AS __union
GROUP BY `node_type`
ORDER BY `node_type` ASC
