WITH undir_edges_a_b_test_integration_user_follows_test AS (
    SELECT e.follower_id, e.followed_id, e.follow_date, e.follow_id, e.follower_id AS __cg_orig_from, e.followed_id AS __cg_orig_to FROM test_integration.user_follows_test AS e
    UNION ALL
    SELECT e.followed_id AS follower_id, e.follower_id AS followed_id, e.follow_date, e.follow_id, e.follower_id AS __cg_orig_from, e.followed_id AS __cg_orig_to FROM test_integration.user_follows_test AS e
)
SELECT 
      a.full_name AS `a.name`, 
      b.full_name AS `b.name`
FROM test_integration.users_test AS a
INNER JOIN undir_edges_a_b_test_integration_user_follows_test AS r1 ON a.user_id = r1.follower_id
INNER JOIN undir_edges_a_b_test_integration_user_follows_test AS r2 ON r1.followed_id = r2.follower_id
INNER JOIN test_integration.users_test AS b ON r2.followed_id = b.user_id
WHERE NOT (r1.__cg_orig_from = r2.__cg_orig_from AND r1.__cg_orig_to = r2.__cg_orig_to)
ORDER BY a.full_name ASC, b.full_name ASC
