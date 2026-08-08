SELECT 
      a.user_id AS "a.user_id", 
      b.user_id AS "b.user_id"
FROM test_integration.users_test AS a
LEFT JOIN (SELECT t0.follower_id AS __cg_combined_anchor_key, b.age, b.city, b.country, b.email_address, b.full_name, b.is_active, b.registration_date, b.user_id FROM test_integration.user_follows_test AS t0 JOIN test_integration.users_test AS b ON b.user_id = t0.followed_id UNION ALL SELECT t0.followed_id AS __cg_combined_anchor_key, b.age, b.city, b.country, b.email_address, b.full_name, b.is_active, b.registration_date, b.user_id FROM test_integration.user_follows_test AS t0 JOIN test_integration.users_test AS b ON b.user_id = t0.follower_id) AS b ON b.__cg_combined_anchor_key = a.user_id
WHERE a.is_active = true
ORDER BY a.user_id ASC, b.user_id ASC
