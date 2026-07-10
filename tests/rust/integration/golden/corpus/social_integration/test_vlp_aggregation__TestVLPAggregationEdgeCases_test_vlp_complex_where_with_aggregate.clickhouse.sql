WITH RECURSIVE vlp_u1_u2 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        end_node.is_active as end_is_active,
        start_node.is_active as start_is_active
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
        end_node.is_active as end_is_active,
        vp.start_is_active as start_is_active
    FROM vlp_u1_u2 vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_nodes, end_node.user_id)
), 
vlp_u2_u1 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        start_node.is_active as start_is_active,
        end_node.is_active as end_is_active
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
        vp.start_is_active as start_is_active,
        end_node.is_active as end_is_active
    FROM vlp_u2_u1 vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_nodes, end_node.user_id)
)
SELECT `u2.user_id` AS "u2.user_id", count(DISTINCT `p.post_id`) AS "postCount" FROM (
SELECT 
      t.end_id AS "u2.user_id",
      p.post_id AS "p.post_id"
FROM vlp_u1_u2 AS t
INNER JOIN test_integration.posts_test AS t0 ON t0.author_id = t.end_id
INNER JOIN test_integration.posts_test AS p ON p.post_id = t0.post_id
WHERE (t.start_id = 1 AND t.end_is_active = true)
UNION ALL 
SELECT 
      t.start_id AS "u2.user_id",
      p.post_id AS "p.post_id"
FROM vlp_u2_u1 AS t
INNER JOIN test_integration.posts_test AS t0 ON t0.author_id = t.start_id
INNER JOIN test_integration.posts_test AS p ON p.post_id = t0.post_id
WHERE (t.end_id = 1 AND t.start_is_active = true)
) AS __union
GROUP BY `u2.user_id`
ORDER BY postCount DESC
LIMIT 5