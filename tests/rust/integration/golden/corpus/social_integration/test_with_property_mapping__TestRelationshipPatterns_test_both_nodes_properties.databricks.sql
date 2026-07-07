WITH with_aName_bName_cte_0 AS (SELECT 
      a.full_name AS `aName`, 
      b.full_name AS `bName`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users_test AS b ON b.user_id = t0.followed_id
)
SELECT 
      aName_bName.aName AS `aName`, 
      aName_bName.bName AS `bName`
FROM with_aName_bName_cte_0 AS aName_bName
LIMIT 3