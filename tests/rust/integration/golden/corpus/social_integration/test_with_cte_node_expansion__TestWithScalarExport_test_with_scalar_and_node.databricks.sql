WITH with_a_follower_count_cte_0 AS (SELECT 
      any_value(a.age) AS `p1_a_age`, 
      any_value(a.city) AS `p1_a_city`, 
      any_value(a.country) AS `p1_a_country`, 
      any_value(a.email_address) AS `p1_a_email`, 
      any_value(a.is_active) AS `p1_a_is_active`, 
      any_value(a.full_name) AS `p1_a_name`, 
      any_value(a.registration_date) AS `p1_a_registration_date`, 
      a.user_id AS `p1_a_user_id`, 
      count(t0.followed_id) AS `follower_count`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
GROUP BY a.user_id
)
SELECT 
      a_follower_count.p1_a_age AS `a.age`, 
      a_follower_count.p1_a_city AS `a.city`, 
      a_follower_count.p1_a_country AS `a.country`, 
      a_follower_count.p1_a_email AS `a.email`, 
      a_follower_count.p1_a_is_active AS `a.is_active`, 
      a_follower_count.p1_a_name AS `a.name`, 
      a_follower_count.p1_a_registration_date AS `a.registration_date`, 
      a_follower_count.p1_a_user_id AS `a.user_id`, 
      a_follower_count.follower_count AS `follower_count`
FROM with_a_follower_count_cte_0 AS a_follower_count
LIMIT 1