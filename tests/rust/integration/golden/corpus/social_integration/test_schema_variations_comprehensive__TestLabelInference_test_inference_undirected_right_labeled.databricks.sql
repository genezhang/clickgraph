WITH vlp_multi_type_a_b AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, string(p2.author_id) AS r_from_id, string(p2.post_id) AS r_to_id, 1 AS hop_count, array('AUTHORED') AS path_relationships, array('{}') AS rel_properties, array(string(a_1.user_id), string(p2.post_id)) AS path_nodes
FROM test_integration.users_test a_1
INNER JOIN test_integration.posts_test p2 ON a_1.user_id = p2.author_id
UNION ALL
SELECT 'Post' AS end_type, p2.post_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, string(r1.user_id) AS r_from_id, string(r1.post_id) AS r_to_id, 1 AS hop_count, array('LIKED') AS path_relationships, array(to_json(struct(r1.like_date))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id)) AS path_nodes
FROM test_integration.users_test a_1
INNER JOIN test_integration.post_likes_test r1 ON a_1.user_id = r1.user_id
INNER JOIN test_integration.posts_test p2 ON r1.post_id = p2.post_id
)
SELECT 
      count(*) AS `total`
FROM vlp_multi_type_a_b AS t
