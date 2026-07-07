SELECT 
      (f.interaction_count / 100) AS "f.relationship_strength"
FROM test_integration.users_expressions_test AS u1
INNER JOIN test_integration.follows_expressions_test AS f ON f.follower_id = u1.user_id
WHERE (u1.user_id = 1 AND f.followed_id = 2)
