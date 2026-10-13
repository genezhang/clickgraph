WITH optional_o1 AS (
SELECT 
      v1.follower_id AS `v0__user_id`, 
      v1.follow_id AS `v1__follow_id`, 
      v1.follower_id AS `v1__follower_id`, 
      v1.followed_id AS `v1__followed_id`, 
      v2.user_id AS `v2__user_id`
FROM test_integration.user_follows_test AS v1
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
)
SELECT 
      v0.city AS `a.city`, 
      count(o1.v1__follow_id) AS `rel_count`
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
GROUP BY v0.city
