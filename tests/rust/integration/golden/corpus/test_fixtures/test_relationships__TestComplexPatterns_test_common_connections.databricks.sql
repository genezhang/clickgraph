SELECT 
      v2.name AS `common.name`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.follows AS v3 ON v3.followed_id = v2.user_id
JOIN test_integration.users AS v4 ON v3.follower_id = v4.user_id
WHERE ((v1.follower_id <> v3.follower_id OR v1.followed_id <> v3.followed_id) AND ((v0.name = 'Alice' AND v4.name = 'Bob') AND v0.name < v4.name))
ORDER BY v2.name ASC NULLS LAST
