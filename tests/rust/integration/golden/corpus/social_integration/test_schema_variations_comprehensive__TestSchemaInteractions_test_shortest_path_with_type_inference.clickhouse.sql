WITH RECURSIVE v0_labels AS (
SELECT 
      'Post' AS "__cg_label", 
      e.post_id AS "__cg_id_0", 
      NULL AS "p2_v0_user_id"
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      e.user_id AS "p2_v0_user_id"
FROM test_integration.users_test AS e
), 
v2_labels AS (
SELECT 
      'Post' AS "__cg_label", 
      e.post_id AS "__cg_id_0", 
      NULL AS "p2_v2_user_id"
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      e.user_id AS "p2_v2_user_id"
FROM test_integration.users_test AS e
), 
vlp_v1_nodes AS (
SELECT 
      'Post' AS "__cg_label", 
      e.post_id AS "__cg_id_0", 
      concat('Post', ':', toString(e.post_id)) AS "__cg_key"
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      concat('User', ':', toString(e.user_id)) AS "__cg_key"
FROM test_integration.users_test AS e
), 
v1_rels AS (
SELECT 
      'AUTHORED' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.author_id AS "__cg_rid_0", 
      e.post_id AS "__cg_rid_1", 
      e.author_id AS "__cg_from_0", 
      e.post_id AS "__cg_to_0", 
      e.author_id AS "__cg_start_0", 
      e.post_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('AUTHORED:User:Post:', toString(tuple(e.author_id, e.post_id))) AS "__cg_key", 
      concat('User', ':', toString(e.author_id)) AS "__cg_start_key", 
      concat('Post', ':', toString(e.post_id)) AS "__cg_end_key"
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'AUTHORED' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.author_id AS "__cg_rid_0", 
      e.post_id AS "__cg_rid_1", 
      e.author_id AS "__cg_from_0", 
      e.post_id AS "__cg_to_0", 
      e.post_id AS "__cg_start_0", 
      e.author_id AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'User' AS "__cg_end_label", 
      concat('AUTHORED:User:Post:', toString(tuple(e.author_id, e.post_id))) AS "__cg_key", 
      concat('Post', ':', toString(e.post_id)) AS "__cg_start_key", 
      concat('User', ':', toString(e.author_id)) AS "__cg_end_key"
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'User' AS "__cg_to_label", 
      e.follow_id AS "__cg_rid_0", 
      NULL AS "__cg_rid_1", 
      e.follower_id AS "__cg_from_0", 
      e.followed_id AS "__cg_to_0", 
      e.follower_id AS "__cg_start_0", 
      e.followed_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'User' AS "__cg_end_label", 
      concat('FOLLOWS:User:User:', toString(e.follow_id)) AS "__cg_key", 
      concat('User', ':', toString(e.follower_id)) AS "__cg_start_key", 
      concat('User', ':', toString(e.followed_id)) AS "__cg_end_key"
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'User' AS "__cg_to_label", 
      e.follow_id AS "__cg_rid_0", 
      NULL AS "__cg_rid_1", 
      e.follower_id AS "__cg_from_0", 
      e.followed_id AS "__cg_to_0", 
      e.followed_id AS "__cg_start_0", 
      e.follower_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'User' AS "__cg_end_label", 
      concat('FOLLOWS:User:User:', toString(e.follow_id)) AS "__cg_key", 
      concat('User', ':', toString(e.followed_id)) AS "__cg_start_key", 
      concat('User', ':', toString(e.follower_id)) AS "__cg_end_key"
FROM test_integration.user_follows_test AS e
WHERE e.follower_id <> e.followed_id

UNION ALL
SELECT 
      'LIKED' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.like_id AS "__cg_rid_0", 
      NULL AS "__cg_rid_1", 
      e.user_id AS "__cg_from_0", 
      e.post_id AS "__cg_to_0", 
      e.user_id AS "__cg_start_0", 
      e.post_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('LIKED:User:Post:', toString(e.like_id)) AS "__cg_key", 
      concat('User', ':', toString(e.user_id)) AS "__cg_start_key", 
      concat('Post', ':', toString(e.post_id)) AS "__cg_end_key"
FROM test_integration.post_likes_test AS e

UNION ALL
SELECT 
      'LIKED' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.like_id AS "__cg_rid_0", 
      NULL AS "__cg_rid_1", 
      e.user_id AS "__cg_from_0", 
      e.post_id AS "__cg_to_0", 
      e.post_id AS "__cg_start_0", 
      e.user_id AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'User' AS "__cg_end_label", 
      concat('LIKED:User:Post:', toString(e.like_id)) AS "__cg_key", 
      concat('Post', ':', toString(e.post_id)) AS "__cg_start_key", 
      concat('User', ':', toString(e.user_id)) AS "__cg_end_key"
FROM test_integration.post_likes_test AS e
), 
vlp_v1_bfs AS (
    SELECT DISTINCT start_node.__cg_key AS start_id, start_node.__cg_key AS node, CAST(0 AS UInt32) AS depth, CAST(1 AS UInt8) AS new
    FROM vlp_v1_nodes AS start_node
    WHERE start_node.__cg_key IS NOT NULL AND ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      v0.__cg_label AS "label", 
      v0.__cg_id_0 AS "id"
FROM v0_labels AS v0
WHERE v0.p2_v0_user_id = 1))
    UNION ALL
    SELECT DISTINCT f.start_id AS start_id, end_node.__cg_key AS node, CAST(f.depth + 1 AS UInt32) AS depth, CAST(1 AS UInt8) AS new
    FROM vlp_v1_bfs AS f
    JOIN v1_rels AS rel ON rel.__cg_start_key = f.node
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_key = rel.__cg_end_key
    WHERE f.new = 1
      AND (f.start_id, end_node.__cg_key) NOT IN (SELECT start_id, node FROM vlp_v1_bfs)
      AND f.start_id NOT IN (SELECT start_id FROM vlp_v1_bfs GROUP BY start_id HAVING countIf(node IN (SELECT end_node.__cg_key FROM vlp_v1_nodes AS end_node WHERE ((end_node.__cg_label, end_node.__cg_id_0) IN (SELECT DISTINCT 
      v2.__cg_label AS "label", 
      v2.__cg_id_0 AS "id"
FROM v2_labels AS v2
WHERE v2.p2_v2_user_id = 10)))) >= (SELECT count(DISTINCT end_node.__cg_key) FROM (SELECT end_node.__cg_key FROM vlp_v1_nodes AS end_node WHERE ((end_node.__cg_label, end_node.__cg_id_0) IN (SELECT DISTINCT 
      v2.__cg_label AS "label", 
      v2.__cg_id_0 AS "id"
FROM v2_labels AS v2
WHERE v2.p2_v2_user_id = 10))) AS end_node))
    UNION ALL
    SELECT start_id, node, depth, CAST(0 AS UInt8) AS new
    FROM vlp_v1_bfs
    WHERE start_id IN (SELECT start_id FROM vlp_v1_bfs WHERE new = 1)
), 
vlp_v1_path AS (
    SELECT start_id, node AS end_id, depth AS hop_count FROM vlp_v1_bfs
    WHERE new = 1 AND depth >= 1
      AND node IN (SELECT end_node.__cg_key FROM vlp_v1_nodes AS end_node WHERE ((end_node.__cg_label, end_node.__cg_id_0) IN (SELECT DISTINCT 
      v2.__cg_label AS "label", 
      v2.__cg_id_0 AS "id"
FROM v2_labels AS v2
WHERE v2.p2_v2_user_id = 10)))
), 
vlp_v1_ends AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, p.hop_count AS hop_count
    FROM vlp_v1_path AS p
    JOIN vlp_v1_nodes AS start_node ON start_node.__cg_key = p.start_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_key = p.end_id
)
SELECT 
      v1.hop_count AS "hops"
FROM vlp_v1_ends AS v1
JOIN v0_labels AS v0 ON v1.start_label = v0.__cg_label AND v1.start_id = v0.__cg_id_0
JOIN v2_labels AS v2 ON v1.end_label = v2.__cg_label AND v1.end_id = v2.__cg_id_0
WHERE (v0.p2_v0_user_id = 1 AND v2.p2_v2_user_id = 10)

-- column hops: Value [("hops", "hops")]