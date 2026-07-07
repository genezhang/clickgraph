WITH vlp_multi_type_a_b AS (
SELECT 'Post' AS end_type, p2.post_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, string(r1.user_id) AS r_from_id, string(r1.post_id) AS r_to_id, 1 AS hop_count, array('AUTHORED') AS path_relationships, array(to_json(struct(r1.authored_date))) AS rel_properties, array(string(a_1.user_id), string(p2.post_id)) AS path_nodes
FROM social.users_bench a_1
INNER JOIN social.authored_bench r1 ON a_1.user_id = r1.user_id
INNER JOIN social.posts_bench p2 ON r1.post_id = p2.post_id
), 
vlp_multi_type_a_b_2 AS (
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, string(r1.follower_id) AS r_from_id, string(r1.followed_id) AS r_to_id, 1 AS hop_count, array('FOLLOWS') AS path_relationships, array(to_json(struct(r1.follow_date))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id)) AS path_nodes
FROM social.users_bench a_1
INNER JOIN social.user_follows_bench r1 ON a_1.user_id = r1.follower_id
INNER JOIN social.users_bench u2 ON r1.followed_id = u2.user_id
)
SELECT 
      element_at(t.path_relationships, 1) AS `t`
FROM vlp_multi_type_a_b AS t
UNION ALL 
SELECT 
      element_at(t.path_relationships, 1) AS `t`
FROM vlp_multi_type_a_b_2 AS t
