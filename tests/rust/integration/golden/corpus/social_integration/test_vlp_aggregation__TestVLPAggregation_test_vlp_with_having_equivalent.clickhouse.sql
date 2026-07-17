WITH RECURSIVE undir_edges_u1_u2_test_integration_user_follows_test AS (
    SELECT e.follower_id, e.followed_id, e.follow_date, e.follow_id, e.follower_id AS __cg_orig_from, e.followed_id AS __cg_orig_to FROM test_integration.user_follows_test AS e
    UNION ALL
    SELECT e.followed_id AS follower_id, e.follower_id AS followed_id, e.follow_date, e.follow_id, e.follower_id AS __cg_orig_from, e.followed_id AS __cg_orig_to FROM test_integration.user_follows_test AS e
),
vlp_u1_u2 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges
    FROM test_integration.users_test AS start_node
    JOIN undir_edges_u1_u2_test_integration_user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.user_id = 1
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_u1_u2 vp
    JOIN undir_edges_u1_u2_test_integration_user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
with_postCount_u2_cte_0 AS (SELECT 
      end_id AS "p2_u2_user_id", 
      count(DISTINCT t0.post_id) AS "postCount"
FROM vlp_u1_u2 AS t
INNER JOIN test_integration.posts_test AS t0 ON t0.author_id = t.end_id
GROUP BY u2.user_id
HAVING postCount > 0
)
SELECT 
      postCount_u2.p2_u2_user_id AS "userId", 
      postCount_u2.postCount AS "postCount"
FROM with_postCount_u2_cte_0 AS postCount_u2
ORDER BY postCount_u2.postCount DESC
LIMIT 5