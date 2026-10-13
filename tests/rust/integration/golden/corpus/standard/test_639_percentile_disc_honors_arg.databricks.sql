SELECT 
      try_element_at(array_sort(collect_list(v2.user_id)), greatest(1, cast(ceil(0.9 * count(v2.user_id)) as int))) AS `percentileDisc(b.user_id, 0.9)`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
