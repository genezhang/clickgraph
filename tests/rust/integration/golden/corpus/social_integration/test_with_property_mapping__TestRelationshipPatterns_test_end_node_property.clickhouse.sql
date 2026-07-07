WITH with_followedName_cte_0 AS (SELECT 
      b.full_name AS "followedName"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users_test AS b ON b.user_id = t0.followed_id
)
SELECT 
      followedName.followedName AS "followedName"
FROM with_followedName_cte_0 AS followedName
LIMIT 3