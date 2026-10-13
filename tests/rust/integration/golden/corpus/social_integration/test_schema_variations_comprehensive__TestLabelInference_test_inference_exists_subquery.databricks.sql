WITH subquery_s1 AS (
SELECT 
      v1.author_id AS `v0__user_id`, 
      CAST(count(*) > 0 AS BOOLEAN) AS `__value`
FROM test_integration.posts_test AS v1
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
GROUP BY v1.author_id
)
SELECT 
      count(*) AS `total`
FROM test_integration.users_test AS v0
LEFT JOIN subquery_s1 AS s1 ON v0.user_id = s1.v0__user_id
WHERE coalesce(s1.__value, false)

-- column total: Value [("total", "total")]