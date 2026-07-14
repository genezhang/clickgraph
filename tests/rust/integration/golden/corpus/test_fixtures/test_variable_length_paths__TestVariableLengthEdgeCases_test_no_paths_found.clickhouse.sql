SELECT 
      a.name AS "a.name", 
      b.name AS "b.name"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS r1 ON a.user_id = r1.follower_id
INNER JOIN test_integration.follows AS r2 ON r1.followed_id = r2.follower_id
INNER JOIN test_integration.follows AS r3 ON r2.followed_id = r3.follower_id
INNER JOIN test_integration.follows AS r4 ON r3.followed_id = r4.follower_id
INNER JOIN test_integration.follows AS r5 ON r4.followed_id = r5.follower_id
INNER JOIN test_integration.users AS b ON r5.followed_id = b.user_id
WHERE (a.name = 'Diana' AND (((((((((NOT (r1.follower_id = r2.follower_id AND r1.followed_id = r2.followed_id) AND NOT (r1.follower_id = r3.follower_id AND r1.followed_id = r3.followed_id)) AND NOT (r1.follower_id = r4.follower_id AND r1.followed_id = r4.followed_id)) AND NOT (r1.follower_id = r5.follower_id AND r1.followed_id = r5.followed_id)) AND NOT (r2.follower_id = r3.follower_id AND r2.followed_id = r3.followed_id)) AND NOT (r2.follower_id = r4.follower_id AND r2.followed_id = r4.followed_id)) AND NOT (r2.follower_id = r5.follower_id AND r2.followed_id = r5.followed_id)) AND NOT (r3.follower_id = r4.follower_id AND r3.followed_id = r4.followed_id)) AND NOT (r3.follower_id = r5.follower_id AND r3.followed_id = r5.followed_id)) AND NOT (r4.follower_id = r5.follower_id AND r4.followed_id = r5.followed_id)))
