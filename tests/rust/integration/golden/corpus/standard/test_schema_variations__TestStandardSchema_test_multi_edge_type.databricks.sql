WITH v2_labels AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`, 
      NULL AS `p2_v2_age`, 
      e.author_id AS `p2_v2_author_id`, 
      NULL AS `p2_v2_city`, 
      e.post_content AS `p2_v2_content`, 
      NULL AS `p2_v2_country`, 
      e.post_date AS `p2_v2_created_at`, 
      NULL AS `p2_v2_email`, 
      NULL AS `p2_v2_is_active`, 
      NULL AS `p2_v2_name`, 
      e.post_id AS `p2_v2_post_id`, 
      NULL AS `p2_v2_registration_date`, 
      e.post_title AS `p2_v2_title`, 
      NULL AS `p2_v2_user_id`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      e.age AS `p2_v2_age`, 
      NULL AS `p2_v2_author_id`, 
      e.city AS `p2_v2_city`, 
      NULL AS `p2_v2_content`, 
      e.country AS `p2_v2_country`, 
      NULL AS `p2_v2_created_at`, 
      e.email_address AS `p2_v2_email`, 
      e.is_active AS `p2_v2_is_active`, 
      e.full_name AS `p2_v2_name`, 
      NULL AS `p2_v2_post_id`, 
      e.registration_date AS `p2_v2_registration_date`, 
      NULL AS `p2_v2_title`, 
      e.user_id AS `p2_v2_user_id`
FROM test_integration.users_test AS e
), 
v1_rels AS (
SELECT 
      'AUTHORED' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.author_id AS `__cg_rid_0`, 
      e.post_id AS `__cg_rid_1`, 
      e.author_id AS `__cg_from_0`, 
      e.post_id AS `__cg_to_0`, 
      e.author_id AS `__cg_start_0`, 
      e.post_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('AUTHORED:User:Post:', to_json(named_struct('_1', CAST(e.author_id AS STRING), '_2', CAST(e.post_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'FOLLOWS' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'User' AS `__cg_to_label`, 
      e.follow_id AS `__cg_rid_0`, 
      NULL AS `__cg_rid_1`, 
      e.follower_id AS `__cg_from_0`, 
      e.followed_id AS `__cg_to_0`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'User' AS `__cg_end_label`, 
      concat('FOLLOWS:User:User:', CAST(e.follow_id AS STRING)) AS `__cg_key`
FROM test_integration.user_follows_test AS e
)
SELECT 
      to_variant_object(map('elementId', CAST(concat(v2.__cg_label, ':', CAST(v2.__cg_id_0 AS STRING), '-') AS VARIANT), 'labels', CAST(array(v2.__cg_label) AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(v2.p2_v2_age AS VARIANT), 'author_id', CAST(v2.p2_v2_author_id AS VARIANT), 'city', CAST(v2.p2_v2_city AS VARIANT), 'content', CAST(v2.p2_v2_content AS VARIANT), 'country', CAST(v2.p2_v2_country AS VARIANT), 'created_at', CAST(v2.p2_v2_created_at AS VARIANT), 'email', CAST(v2.p2_v2_email AS VARIANT), 'is_active', CAST(v2.p2_v2_is_active AS VARIANT), 'name', CAST(v2.p2_v2_name AS VARIANT), 'post_id', CAST(v2.p2_v2_post_id AS VARIANT), 'registration_date', CAST(v2.p2_v2_registration_date AS VARIANT), 'title', CAST(v2.p2_v2_title AS VARIANT), 'user_id', CAST(v2.p2_v2_user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))) AS `target`
FROM test_integration.users_test AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
WHERE v0.user_id = 1
LIMIT 10