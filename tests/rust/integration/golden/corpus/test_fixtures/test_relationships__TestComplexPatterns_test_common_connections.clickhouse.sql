SELECT 
      common.name AS "common.name"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users AS common ON common.user_id = t0.followed_id
INNER JOIN test_integration.follows AS t1 ON t1.followed_id = common.user_id
INNER JOIN test_integration.users AS b ON b.user_id = t1.follower_id
WHERE ((b.name = 'Bob' AND a.name < b.name) AND a.name = 'Alice')
ORDER BY common.name ASC
