SELECT `a.user_id` AS `a.user_id`, `b.user_id` AS `b.user_id` FROM (
SELECT 
      a.user_id AS "a.user_id", 
      t0.followed_id AS "b.user_id", 
      a.user_id AS "__order_col_0", 
      t0.followed_id AS "__order_col_1"
FROM test_integration.users_test AS a
LEFT JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
WHERE a.is_active = true
UNION ALL 
SELECT 
      a.user_id AS "a.user_id", 
      b.user_id AS "b.user_id", 
      a.user_id AS "__order_col_0", 
      t0.followed_id AS "__order_col_1"
FROM test_integration.users_test AS b
LEFT JOIN (SELECT t0.follower_id AS __cg_combined_anchor_key, a.* FROM test_integration.user_follows_test AS t0 JOIN test_integration.users_test AS a ON a.user_id = t0.followed_id WHERE a.is_active = true) AS a ON a.__cg_combined_anchor_key = b.user_id
) AS __union
ORDER BY __union.`__order_col_0` ASC, __union.`__order_col_1` ASC
