SELECT 
      a.user_id AS "a.id", 
      b.user_id AS "b.id"
FROM test_integration.users_test AS a
LEFT JOIN (SELECT t0.follower_id AS __cg_combined_anchor_key, b.* FROM test_integration.user_follows_test AS t0 JOIN test_integration.users_test AS b ON b.user_id = t0.followed_id WHERE b.country = 'USA') AS b ON b.__cg_combined_anchor_key = a.user_id
