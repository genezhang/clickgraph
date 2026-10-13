WITH v2_labels AS (
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
      concat('AUTHORED:User:Post:', toString(tuple(e.author_id, e.post_id))) AS "__cg_key"
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
      concat('FOLLOWS:User:User:', toString(e.follow_id)) AS "__cg_key"
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
      concat('FOLLOWS:User:User:', toString(e.follow_id)) AS "__cg_key"
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
      concat('LIKED:User:Post:', toString(e.like_id)) AS "__cg_key"
FROM test_integration.post_likes_test AS e
), 
subquery_s3 AS (
SELECT 
      v0.user_id AS "v0__user_id", 
      count(*) AS "__value"
FROM test_integration.users_test AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
WHERE v0.user_id = 1
GROUP BY v0.user_id
), 
with_w4 AS (
SELECT 
      v0.user_id AS "v4__user_id", 
      v0.age AS "p2_v4_age", 
      v0.city AS "p2_v4_city", 
      v0.country AS "p2_v4_country", 
      v0.email_address AS "p2_v4_email", 
      v0.is_active AS "p2_v4_is_active", 
      v0.full_name AS "p2_v4_name", 
      v0.registration_date AS "p2_v4_registration_date", 
      v0.user_id AS "p2_v4_user_id", 
      coalesce(s3.__value, 0) AS "v5"
FROM test_integration.users_test AS v0
LEFT JOIN subquery_s3 AS s3 ON v0.user_id = s3.v0__user_id
WHERE v0.user_id = 1
), 
v7_labels AS (
SELECT 
      'Post' AS "__cg_label", 
      e.post_id AS "__cg_id_0", 
      NULL AS "p2_v7_age", 
      e.author_id AS "p2_v7_author_id", 
      NULL AS "p2_v7_city", 
      e.post_content AS "p2_v7_content", 
      NULL AS "p2_v7_country", 
      e.post_date AS "p2_v7_created_at", 
      NULL AS "p2_v7_email", 
      NULL AS "p2_v7_is_active", 
      NULL AS "p2_v7_name", 
      e.post_id AS "p2_v7_post_id", 
      NULL AS "p2_v7_registration_date", 
      e.post_title AS "p2_v7_title", 
      NULL AS "p2_v7_user_id"
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      e.age AS "p2_v7_age", 
      NULL AS "p2_v7_author_id", 
      e.city AS "p2_v7_city", 
      NULL AS "p2_v7_content", 
      e.country AS "p2_v7_country", 
      NULL AS "p2_v7_created_at", 
      e.email_address AS "p2_v7_email", 
      e.is_active AS "p2_v7_is_active", 
      e.full_name AS "p2_v7_name", 
      NULL AS "p2_v7_post_id", 
      e.registration_date AS "p2_v7_registration_date", 
      NULL AS "p2_v7_title", 
      e.user_id AS "p2_v7_user_id"
FROM test_integration.users_test AS e
), 
v6_rels AS (
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
      NULL AS "p2_v6_follow_date", 
      NULL AS "p2_v6_like_date"
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
      e.follow_date AS "p2_v6_follow_date", 
      NULL AS "p2_v6_like_date"
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
      e.follow_date AS "p2_v6_follow_date", 
      NULL AS "p2_v6_like_date"
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
      NULL AS "p2_v6_follow_date", 
      e.like_date AS "p2_v6_like_date"
FROM test_integration.post_likes_test AS e
)
SELECT 
      [map('elementId', CAST(concat('User:', toString(w4.v4__user_id), '-'), 'Dynamic'), 'labels', CAST(['User'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(w4.p2_v4_age, 'Dynamic'), 'city', CAST(w4.p2_v4_city, 'Dynamic'), 'country', CAST(w4.p2_v4_country, 'Dynamic'), 'email', CAST(w4.p2_v4_email, 'Dynamic'), 'is_active', CAST(w4.p2_v4_is_active, 'Dynamic'), 'name', CAST(w4.p2_v4_name, 'Dynamic'), 'registration_date', CAST(w4.p2_v4_registration_date, 'Dynamic'), 'user_id', CAST(w4.p2_v4_user_id, 'Dynamic'))), 'Dynamic')), map('elementId', CAST(concat(v6.__cg_type, ':', toString(v6.__cg_from_0), '->', toString(v6.__cg_to_0), '-'), 'Dynamic'), 'startNodeElementId', CAST(concat(v6.__cg_from_label, ':', toString(v6.__cg_from_0), '-'), 'Dynamic'), 'endNodeElementId', CAST(concat(v6.__cg_to_label, ':', toString(v6.__cg_to_0), '-'), 'Dynamic'), 'type', CAST(v6.__cg_type, 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('follow_date', CAST(v6.p2_v6_follow_date, 'Dynamic'), 'like_date', CAST(v6.p2_v6_like_date, 'Dynamic'))), 'Dynamic')), map('elementId', CAST(concat(v7.__cg_label, ':', toString(v7.__cg_id_0), '-'), 'Dynamic'), 'labels', CAST([v7.__cg_label], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(v7.p2_v7_age, 'Dynamic'), 'author_id', CAST(v7.p2_v7_author_id, 'Dynamic'), 'city', CAST(v7.p2_v7_city, 'Dynamic'), 'content', CAST(v7.p2_v7_content, 'Dynamic'), 'country', CAST(v7.p2_v7_country, 'Dynamic'), 'created_at', CAST(v7.p2_v7_created_at, 'Dynamic'), 'email', CAST(v7.p2_v7_email, 'Dynamic'), 'is_active', CAST(v7.p2_v7_is_active, 'Dynamic'), 'name', CAST(v7.p2_v7_name, 'Dynamic'), 'post_id', CAST(v7.p2_v7_post_id, 'Dynamic'), 'registration_date', CAST(v7.p2_v7_registration_date, 'Dynamic'), 'title', CAST(v7.p2_v7_title, 'Dynamic'), 'user_id', CAST(v7.p2_v7_user_id, 'Dynamic'))), 'Dynamic'))] AS "path", 
      w4.v5 AS "allNeighboursCount"
FROM with_w4 AS w4
JOIN v6_rels AS v6 ON v6.__cg_start_0 = w4.v4__user_id
JOIN v7_labels AS v7 ON v6.__cg_end_label = v7.__cg_label AND v6.__cg_end_0 = v7.__cg_id_0
ORDER BY v7.__cg_label ASC, v7.__cg_id_0 ASC
LIMIT 100
-- column path: Graph(Path) [("path", "path")]
-- column allNeighboursCount: Value [("allNeighboursCount", "allNeighboursCount")]