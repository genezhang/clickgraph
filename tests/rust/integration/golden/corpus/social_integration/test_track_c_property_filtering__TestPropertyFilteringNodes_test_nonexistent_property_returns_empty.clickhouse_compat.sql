WITH v0_labels AS (
SELECT 
      'Post' AS "__cg_label", 
      e.post_id AS "__cg_id_0", 
      NULL AS "p2_v0_age", 
      e.author_id AS "p2_v0_author_id", 
      NULL AS "p2_v0_city", 
      e.post_content AS "p2_v0_content", 
      NULL AS "p2_v0_country", 
      e.post_date AS "p2_v0_created_at", 
      NULL AS "p2_v0_email", 
      NULL AS "p2_v0_is_active", 
      NULL AS "p2_v0_name", 
      NULL AS "p2_v0_nonexistent_xyz_999", 
      e.post_id AS "p2_v0_post_id", 
      NULL AS "p2_v0_registration_date", 
      e.post_title AS "p2_v0_title", 
      NULL AS "p2_v0_user_id"
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      e.age AS "p2_v0_age", 
      NULL AS "p2_v0_author_id", 
      e.city AS "p2_v0_city", 
      NULL AS "p2_v0_content", 
      e.country AS "p2_v0_country", 
      NULL AS "p2_v0_created_at", 
      e.email_address AS "p2_v0_email", 
      e.is_active AS "p2_v0_is_active", 
      e.full_name AS "p2_v0_name", 
      NULL AS "p2_v0_nonexistent_xyz_999", 
      NULL AS "p2_v0_post_id", 
      e.registration_date AS "p2_v0_registration_date", 
      NULL AS "p2_v0_title", 
      e.user_id AS "p2_v0_user_id"
FROM test_integration.users_test AS e
)
SELECT 
      map('elementId', CAST(concat(v0.__cg_label, ':', toString(v0.__cg_id_0), '-'), 'Dynamic'), 'labels', CAST([v0.__cg_label], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(v0.p2_v0_age, 'Dynamic'), 'author_id', CAST(v0.p2_v0_author_id, 'Dynamic'), 'city', CAST(v0.p2_v0_city, 'Dynamic'), 'content', CAST(v0.p2_v0_content, 'Dynamic'), 'country', CAST(v0.p2_v0_country, 'Dynamic'), 'created_at', CAST(v0.p2_v0_created_at, 'Dynamic'), 'email', CAST(v0.p2_v0_email, 'Dynamic'), 'is_active', CAST(v0.p2_v0_is_active, 'Dynamic'), 'name', CAST(v0.p2_v0_name, 'Dynamic'), 'post_id', CAST(v0.p2_v0_post_id, 'Dynamic'), 'registration_date', CAST(v0.p2_v0_registration_date, 'Dynamic'), 'title', CAST(v0.p2_v0_title, 'Dynamic'), 'user_id', CAST(v0.p2_v0_user_id, 'Dynamic'))), 'Dynamic')) AS "n"
FROM v0_labels AS v0
WHERE v0.p2_v0_nonexistent_xyz_999 = 123

-- column n: Graph(Node) [("n", "n")]