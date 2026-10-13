WITH v0_labels AS (
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
with_w2 AS (
SELECT DISTINCT 
      'node' AS "__cg_w2_c0", 
      v0.p2_v0_user_id AS "__cg_w2_c1"
FROM v0_labels AS v0
WHERE v0.p2_v0_user_id IS NOT NULL
LIMIT 5), 
v5_labels AS (
SELECT 
      'Post' AS "__cg_label", 
      e.post_id AS "__cg_id_0"
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0"
FROM test_integration.users_test AS e
), 
v4_rels AS (
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
      NULL AS "p2_v4_follow_date"
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
      e.follow_date AS "p2_v4_follow_date"
FROM test_integration.user_follows_test AS e

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
      NULL AS "p2_v4_follow_date"
FROM test_integration.post_likes_test AS e
), 
with_w5 AS (
SELECT DISTINCT 
      'relationship' AS "__cg_w5_c0", 
      v4.p2_v4_follow_date AS "__cg_w5_c1"
FROM test_integration.users_test AS v3
JOIN v4_rels AS v4 ON v4.__cg_start_0 = v3.user_id
JOIN v5_labels AS v5 ON v4.__cg_end_label = v5.__cg_label AND v4.__cg_end_0 = v5.__cg_id_0
WHERE v4.p2_v4_follow_date IS NOT NULL
LIMIT 5), 
with_w6 AS (
SELECT 
      CAST(w2.__cg_w2_c0, 'Dynamic') AS "__cg_c0", 
      CAST(w2.__cg_w2_c1, 'Dynamic') AS "__cg_c1"
FROM with_w2 AS w2

UNION ALL
SELECT 
      CAST(w5.__cg_w5_c0, 'Dynamic') AS "__cg_c0", 
      CAST(w5.__cg_w5_c1, 'Dynamic') AS "__cg_c1"
FROM with_w5 AS w5
)
SELECT 
      w6.__cg_c0 AS "entity", 
      w6.__cg_c1 AS "value"
FROM with_w6 AS w6
