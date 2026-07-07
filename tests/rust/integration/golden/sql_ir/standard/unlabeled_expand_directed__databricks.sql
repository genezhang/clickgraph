WITH pattern_union_r AS (
(SELECT 'User' AS start_type, string(social.users_bench.user_id) as start_id, string(social.posts_bench.post_id) as end_id, 'Post' AS end_type, array('AUTHORED') as path_relationships, array(to_json(struct(social.authored_bench.authored_date AS authored_date))) as rel_properties, to_json(struct(social.users_bench.city, social.users_bench.country, social.users_bench.email_address, social.users_bench.is_active, social.users_bench.full_name, social.users_bench.registration_date, social.users_bench.user_id)) as start_properties, to_json(struct(social.posts_bench.author_id, social.posts_bench.post_content, social.posts_bench.post_date, social.posts_bench.post_id, social.posts_bench.post_title)) as end_properties, social.authored_bench.authored_date AS authored_date, NULL AS follow_date, NULL AS like_date FROM social.authored_bench INNER JOIN social.users_bench ON social.users_bench.user_id = social.authored_bench.user_id INNER JOIN social.posts_bench ON social.posts_bench.post_id = social.authored_bench.post_id LIMIT 1000)
UNION ALL
(SELECT 'User' AS start_type, string(from_node.user_id) as start_id, string(to_node.user_id) as end_id, 'User' AS end_type, array('FOLLOWS') as path_relationships, array(to_json(struct(social.user_follows_bench.follow_date AS follow_date))) as rel_properties, to_json(struct(from_node.city, from_node.country, from_node.email_address, from_node.is_active, from_node.full_name, from_node.registration_date, from_node.user_id)) as start_properties, to_json(struct(to_node.city, to_node.country, to_node.email_address, to_node.is_active, to_node.full_name, to_node.registration_date, to_node.user_id)) as end_properties, NULL AS authored_date, social.user_follows_bench.follow_date AS follow_date, NULL AS like_date FROM social.user_follows_bench INNER JOIN social.users_bench AS from_node ON from_node.user_id = social.user_follows_bench.follower_id INNER JOIN social.users_bench AS to_node ON to_node.user_id = social.user_follows_bench.followed_id LIMIT 1000)
UNION ALL
(SELECT 'User' AS start_type, string(social.users_bench.user_id) as start_id, string(social.posts_bench.post_id) as end_id, 'Post' AS end_type, array('LIKED') as path_relationships, array(to_json(struct(social.post_likes_bench.like_date AS like_date))) as rel_properties, to_json(struct(social.users_bench.city, social.users_bench.country, social.users_bench.email_address, social.users_bench.is_active, social.users_bench.full_name, social.users_bench.registration_date, social.users_bench.user_id)) as start_properties, to_json(struct(social.posts_bench.author_id, social.posts_bench.post_content, social.posts_bench.post_date, social.posts_bench.post_id, social.posts_bench.post_title)) as end_properties, NULL AS authored_date, NULL AS follow_date, social.post_likes_bench.like_date AS like_date FROM social.post_likes_bench INNER JOIN social.users_bench ON social.users_bench.user_id = social.post_likes_bench.user_id INNER JOIN social.posts_bench ON social.posts_bench.post_id = social.post_likes_bench.post_id LIMIT 1000)
)
SELECT 
      r.start_properties AS `n.properties`, 
      r.start_id AS `n.id`, 
      r.start_type AS `n.__label__`, 
      r.path_relationships AS `r.type`, 
      r.rel_properties AS `r.properties`, 
      r.start_id AS `r.start_id`, 
      r.end_id AS `r.end_id`, 
      r.end_properties AS `o.properties`, 
      r.end_id AS `o.id`, 
      r.end_type AS `o.__label__`
FROM pattern_union_r AS r
