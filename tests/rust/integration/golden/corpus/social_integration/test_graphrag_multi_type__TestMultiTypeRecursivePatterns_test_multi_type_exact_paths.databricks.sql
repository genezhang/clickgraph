WITH vlp_multi_type_u_x AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, string(p2.author_id) AS r_from_id, string(p2.post_id) AS r_to_id, to_json(struct(u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id)) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, array('AUTHORED') AS path_relationships, array('{}') AS rel_properties, array(string(u_1.user_id), string(p2.post_id)) AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.posts_test p2 ON u_1.user_id = p2.author_id
WHERE (u_1.user_id = 1)
), 
vlp_multi_type_u_x_2 AS (
SELECT 'User' AS end_type, u2.user_id AS end_id, u_1.user_id AS start_id, 'User' AS start_type, string(r1.follower_id) AS r_from_id, string(r1.followed_id) AS r_to_id, to_json(struct(u_1.age, u_1.city, u_1.country, u_1.email_address, u_1.is_active, u_1.full_name, u_1.registration_date, u_1.user_id)) AS start_properties, u_1.user_id AS start_user_id, 1 AS hop_count, array('FOLLOWS') AS path_relationships, array(to_json(struct(r1.follow_date))) AS rel_properties, array(string(u_1.user_id), string(u2.user_id)) AS path_nodes
FROM test_integration.users_test u_1
INNER JOIN test_integration.user_follows_test r1 ON u_1.user_id = r1.follower_id
INNER JOIN test_integration.users_test u2 ON r1.followed_id = u2.user_id
WHERE (u_1.user_id = 1)
)
SELECT `node_type` AS `node_type`, count(*) AS `cnt` FROM (
SELECT 
      t.end_type AS `node_type`
FROM vlp_multi_type_u_x AS t
WHERE t.start_id = 1
UNION ALL 
SELECT 
      t.end_type AS `node_type`
FROM vlp_multi_type_u_x_2 AS t
WHERE t.start_id = 1
) AS __union
GROUP BY `node_type`
ORDER BY `node_type` ASC
