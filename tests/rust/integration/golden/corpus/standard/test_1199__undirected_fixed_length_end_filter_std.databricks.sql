WITH undir_edges_a_b_test_integration_user_follows_test AS (
    SELECT e.follower_id, e.followed_id, e.follow_date, e.follow_id, e.follower_id AS __cg_orig_from, e.followed_id AS __cg_orig_to FROM test_integration.user_follows_test AS e
    UNION ALL
    SELECT e.followed_id AS follower_id, e.follower_id AS followed_id, e.follow_date, e.follow_id, e.follower_id AS __cg_orig_from, e.followed_id AS __cg_orig_to FROM test_integration.user_follows_test AS e
)
SELECT 
      count(*) AS `count(*)`
FROM test_integration.users_test AS a
INNER JOIN undir_edges_a_b_test_integration_user_follows_test AS r1 ON a.user_id = r1.follower_id
INNER JOIN undir_edges_a_b_test_integration_user_follows_test AS r2 ON r1.followed_id = r2.follower_id
WHERE (r2.followed_id = 3 AND r1.follow_id <> r2.follow_id)
