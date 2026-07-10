SELECT 
      splitByChar(',', u.tags_str) AS "u.tags_array", 
      length(splitByChar(',', u.tags_str)) AS "u.tag_count"
FROM test_integration.users_expressions_test AS u
WHERE u.user_id = 6
