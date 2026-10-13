SELECT 
      splitByChar(',', v0.tags_str) AS `u.tags_array`, 
      length(splitByChar(',', v0.tags_str)) AS `u.tag_count`
FROM test_integration.users_expressions_test AS v0
WHERE v0.user_id = 6
