SELECT 
      count(*) AS "n"
FROM test_integration.users_test AS n0
LEFT JOIN (SELECT t0.follower_id AS __cg_combined_anchor_key, n1.* FROM test_integration.user_follows_test AS t0 JOIN test_integration.users_test AS n1 ON n1.user_id = t0.followed_id WHERE n1.age > 30) AS n1 ON n1.__cg_combined_anchor_key = n0.user_id
