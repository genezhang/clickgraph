WITH with_followerName_cte_0 AS (SELECT 
      a.full_name AS "followerName"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
)
SELECT 
      followerName.followerName AS "followerName"
FROM with_followerName_cte_0 AS followerName
LIMIT 3