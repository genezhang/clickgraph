SELECT DISTINCT 
      fof.name AS "fof.name"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.follows AS t1 ON t1.follower_id = t0.followed_id
INNER JOIN test_integration.users AS fof ON fof.user_id = t1.followed_id
WHERE (fof.name <> 'Alice' AND a.name = 'Alice')
ORDER BY fof.name ASC
