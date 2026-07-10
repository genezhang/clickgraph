WITH with_f_user_id_cte_0 AS (SELECT 
      f.email_address AS `p1_f_email`, 
      f.full_name AS `p1_f_name`
FROM test_integration.users_test AS f
INNER JOIN test_integration.user_follows_test AS t0 ON f.user_id = t0.followed_id
INNER JOIN test_integration.users_test AS u ON t0.follower_id = u.user_id
WHERE f.country = 'USA'
)
SELECT 
      f_user_id.p1_f_name AS `f.name`, 
      f_user_id.p1_f_email AS `f.email`
FROM with_f_user_id_cte_0 AS f_user_id
