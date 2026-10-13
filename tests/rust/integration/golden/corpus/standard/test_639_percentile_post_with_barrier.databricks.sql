WITH with_w1 AS (
SELECT 
      v0.city AS `v3`, 
      try_element_at(array_sort(collect_list(v2.user_id)), greatest(1, cast(ceil(0.95 * count(v2.user_id)) as int))) AS `v4`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
GROUP BY v0.city
)
SELECT 
      w1.v3 AS `city`, 
      w1.v4 AS `p`
FROM with_w1 AS w1

-- column city: Value [("city", "city")]
-- column p: Value [("p", "p")]