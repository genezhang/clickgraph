SELECT 
      count(*) AS "recent_follows"
FROM test_integration.follows_expressions_test AS f
WHERE (dateDiff('day', f.follow_date, today()) <= 7) = true
