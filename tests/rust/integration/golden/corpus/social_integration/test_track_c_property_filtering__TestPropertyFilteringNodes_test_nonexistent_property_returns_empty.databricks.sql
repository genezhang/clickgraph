WITH v0_labels AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`, 
      NULL AS `p2_v0_age`, 
      e.author_id AS `p2_v0_author_id`, 
      NULL AS `p2_v0_city`, 
      e.post_content AS `p2_v0_content`, 
      NULL AS `p2_v0_country`, 
      e.post_date AS `p2_v0_created_at`, 
      NULL AS `p2_v0_email`, 
      NULL AS `p2_v0_is_active`, 
      NULL AS `p2_v0_name`, 
      e.nonexistent_xyz_999 AS `p2_v0_nonexistent_xyz_999__cg0`, 
      NULL AS `p2_v0_nonexistent_xyz_999__cg1`, 
      e.post_id AS `p2_v0_post_id`, 
      NULL AS `p2_v0_registration_date`, 
      e.post_title AS `p2_v0_title`, 
      NULL AS `p2_v0_user_id`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      e.age AS `p2_v0_age`, 
      NULL AS `p2_v0_author_id`, 
      e.city AS `p2_v0_city`, 
      NULL AS `p2_v0_content`, 
      e.country AS `p2_v0_country`, 
      NULL AS `p2_v0_created_at`, 
      e.email_address AS `p2_v0_email`, 
      e.is_active AS `p2_v0_is_active`, 
      e.full_name AS `p2_v0_name`, 
      NULL AS `p2_v0_nonexistent_xyz_999__cg0`, 
      e.nonexistent_xyz_999 AS `p2_v0_nonexistent_xyz_999__cg1`, 
      NULL AS `p2_v0_post_id`, 
      e.registration_date AS `p2_v0_registration_date`, 
      NULL AS `p2_v0_title`, 
      e.user_id AS `p2_v0_user_id`
FROM test_integration.users_test AS e
)
SELECT 
      to_variant_object(map('elementId', CAST(concat(v0.__cg_label, ':', CAST(v0.__cg_id_0 AS STRING), '-') AS VARIANT), 'labels', CAST(array(v0.__cg_label) AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(v0.p2_v0_age AS VARIANT), 'author_id', CAST(v0.p2_v0_author_id AS VARIANT), 'city', CAST(v0.p2_v0_city AS VARIANT), 'content', CAST(v0.p2_v0_content AS VARIANT), 'country', CAST(v0.p2_v0_country AS VARIANT), 'created_at', CAST(v0.p2_v0_created_at AS VARIANT), 'email', CAST(v0.p2_v0_email AS VARIANT), 'is_active', CAST(v0.p2_v0_is_active AS VARIANT), 'name', CAST(v0.p2_v0_name AS VARIANT), 'post_id', CAST(v0.p2_v0_post_id AS VARIANT), 'registration_date', CAST(v0.p2_v0_registration_date AS VARIANT), 'title', CAST(v0.p2_v0_title AS VARIANT), 'user_id', CAST(v0.p2_v0_user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))) AS `n`
FROM v0_labels AS v0
WHERE CASE WHEN regexp_replace(regexp_replace(typeof(v0.p2_v0_nonexistent_xyz_999__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v0.p2_v0_nonexistent_xyz_999__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v0.p2_v0_nonexistent_xyz_999__cg0, v0.p2_v0_nonexistent_xyz_999__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END = 123
