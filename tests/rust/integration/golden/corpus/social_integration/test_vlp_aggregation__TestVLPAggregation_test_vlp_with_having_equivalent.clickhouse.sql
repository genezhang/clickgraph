WITH RECURSIVE v1_both AS (
SELECT 
      e.follow_date AS "follow_date", 
      e.follow_id AS "follow_id", 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.follower_id AS "__cg_start_0", 
      e.followed_id AS "__cg_end_0"
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      e.follow_date AS "follow_date", 
      e.follow_id AS "follow_id", 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.followed_id AS "__cg_start_0", 
      e.follower_id AS "__cg_end_0"
FROM test_integration.user_follows_test AS e
WHERE e.follower_id <> e.followed_id
), 
vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges
    FROM test_integration.users_test AS start_node
    JOIN v1_both AS rel ON start_node.user_id = rel.__cg_start_0
    JOIN test_integration.users_test AS end_node ON rel.__cg_end_0 = end_node.user_id
    WHERE (start_node.user_id = 1)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_v1_path vp
    JOIN v1_both AS rel ON vp.end_id = rel.__cg_start_0
    JOIN test_integration.users_test AS end_node ON rel.__cg_end_0 = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
with_w3 AS (
SELECT 
      v2.user_id AS "v5__user_id", 
      v2.user_id AS "p2_v5_user_id", 
      count(DISTINCT v4.post_id) AS "v6"
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id
JOIN test_integration.posts_test AS v3 ON v3.author_id = v2.user_id
JOIN test_integration.posts_test AS v4 ON v3.post_id = v4.post_id
WHERE v0.user_id = 1
GROUP BY v2.user_id, v2.user_id
HAVING count(DISTINCT v4.post_id) > 0
)
SELECT 
      w3.p2_v5_user_id AS "userId", 
      w3.v6 AS "postCount"
FROM with_w3 AS w3
ORDER BY w3.v6 DESC NULLS FIRST
LIMIT 5
-- column userId: Value [("userId", "userId")]
-- column postCount: Value [("postCount", "postCount")]