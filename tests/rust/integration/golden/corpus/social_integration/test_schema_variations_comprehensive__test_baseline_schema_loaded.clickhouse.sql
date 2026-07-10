SELECT count(coalesce(`n.post_id`, `n.user_id`)) AS "count(n)" FROM (
SELECT 
      NULL AS "age",
      toString(n.author_id) AS "author_id",
      NULL AS "city",
      toString(n.post_content) AS "content",
      NULL AS "country",
      toString(n.post_date) AS "created_at",
      NULL AS "email",
      NULL AS "is_active",
      NULL AS "name",
      toString(n.post_id) AS "post_id",
      NULL AS "registration_date",
      toString(n.post_title) AS "title",
      NULL AS "user_id",
      toString(n.post_id) AS "n.post_id",
      NULL AS "n.user_id"
FROM test_integration.posts_test AS n
UNION ALL 
SELECT 
      toString(n.age) AS "age",
      NULL AS "author_id",
      toString(n.city) AS "city",
      NULL AS "content",
      toString(n.country) AS "country",
      NULL AS "created_at",
      toString(n.email_address) AS "email",
      toString(n.is_active) AS "is_active",
      toString(n.full_name) AS "name",
      NULL AS "post_id",
      toString(n.registration_date) AS "registration_date",
      NULL AS "title",
      toString(n.user_id) AS "user_id",
      NULL AS "n.post_id",
      toString(n.user_id) AS "n.user_id"
FROM test_integration.users_test AS n
) AS __union
LIMIT 1