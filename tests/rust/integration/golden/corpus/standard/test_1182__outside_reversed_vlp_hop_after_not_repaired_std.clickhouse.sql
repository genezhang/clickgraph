WITH RECURSIVE with_c_cte_0 AS (SELECT 
      t0.followed_id AS "p1_c_user_id"
FROM test_integration.users_test AS z
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = z.user_id
), 
vlp_n2_n1 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_n2_n1 vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
)
SELECT 
      c.p1_c_user_id AS "c.user_id", 
      t1.follower_id AS "n3.user_id"
FROM vlp_n2_n1 AS t
INNER JOIN test_integration.user_follows_test AS t2 ON t2.follower_id = t.start_id
INNER JOIN test_integration.users_test AS n1 ON t.end_id = t2.followed_id
INNER JOIN test_integration.user_follows_test AS t1 ON t1.followed_id = t.start_id
INNER JOIN test_integration.users_test AS n2 ON t.start_id = t1.followed_id
INNER JOIN with_c_cte_0 AS c ON t1.follow_id <> t3.follow_id
INNER JOIN test_integration.user_follows_test AS t3 ON t3.follower_id = c.p1_c_user_id
