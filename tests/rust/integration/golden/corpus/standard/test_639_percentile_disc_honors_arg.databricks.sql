SELECT 
      try_element_at(array_sort(collect_list(t0.followed_id)), greatest(1, cast(ceil(0.9 * count(t0.followed_id)) as int))) AS `percentileDisc(b.user_id, 0.9)`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
