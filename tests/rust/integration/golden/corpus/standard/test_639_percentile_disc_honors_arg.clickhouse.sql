SELECT 
      arrayElementOrNull(arraySort(groupArray(t0.followed_id)), greatest(1, toUInt32(ceil(0.9 * count(t0.followed_id))))) AS "percentileDisc(b.user_id, 0.9)"
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
