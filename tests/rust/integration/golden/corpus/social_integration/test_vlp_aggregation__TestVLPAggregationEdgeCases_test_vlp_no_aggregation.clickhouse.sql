WITH RECURSIVE vlp_u1_u2 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges,
        end_node.full_name as end_name,
        start_node.full_name as start_name
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.user_id = 1
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges,
        end_node.full_name as end_name,
        vp.start_name as start_name
    FROM vlp_u1_u2 vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
vlp_u2_u1_inner AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges,
        start_node.full_name as start_name,
        end_node.full_name as end_name
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
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges,
        vp.start_name as start_name,
        end_node.full_name as end_name
    FROM vlp_u2_u1_inner vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
),
vlp_u2_u1 AS (
    SELECT * FROM vlp_u2_u1_inner WHERE (end_id = 1)
)
SELECT `u2.user_id` AS `u2.user_id`, `u2.name` AS `u2.name`, `p.post_id` AS `p.post_id` FROM (
SELECT 
      t.end_id AS "u2.user_id", 
      t.end_name AS "u2.name", 
      t0.post_id AS "p.post_id"
FROM vlp_u1_u2 AS t
INNER JOIN test_integration.posts_test AS t0 ON t0.author_id = t.end_id
UNION ALL 
SELECT 
      t.start_id AS "u2.user_id", 
      t.start_name AS "u2.name", 
      t0.post_id AS "p.post_id"
FROM vlp_u2_u1 AS t
INNER JOIN test_integration.posts_test AS t0 ON t0.author_id = t.start_id
) AS __union
LIMIT 10