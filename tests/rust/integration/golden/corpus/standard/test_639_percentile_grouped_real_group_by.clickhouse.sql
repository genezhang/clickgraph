SELECT 
      a.city AS "a.city", 
      quantileExactInclusive(0.5)(t0.followed_id) AS "percentileCont(b.user_id, 0.5)"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
GROUP BY a.city
