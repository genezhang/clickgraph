SELECT 
      a.city AS "a.city", 
      count(r.follow_id) AS "rel_count"
FROM test_integration.users_test AS a
LEFT JOIN test_integration.user_follows_test AS r ON r.follower_id = a.user_id
GROUP BY a.city
