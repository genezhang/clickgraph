WITH v2_labels AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`, 
      e.status AS `p2_v2_status__cg0`, 
      NULL AS `p2_v2_status__cg1`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      NULL AS `p2_v2_status__cg0`, 
      e.status AS `p2_v2_status__cg1`
FROM test_integration.users_test AS e
), 
v1_rels AS (
SELECT 
      'AUTHORED' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.author_id AS `__cg_rid_0`, 
      e.post_id AS `__cg_rid_1`, 
      e.author_id AS `__cg_from_0`, 
      e.post_id AS `__cg_to_0`, 
      e.author_id AS `__cg_start_0`, 
      e.post_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('AUTHORED:User:Post:', to_json(named_struct('_1', CAST(e.author_id AS STRING), '_2', CAST(e.post_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'FOLLOWS' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'User' AS `__cg_to_label`, 
      e.follow_id AS `__cg_rid_0`, 
      NULL AS `__cg_rid_1`, 
      e.follower_id AS `__cg_from_0`, 
      e.followed_id AS `__cg_to_0`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'User' AS `__cg_end_label`, 
      concat('FOLLOWS:User:User:', CAST(e.follow_id AS STRING)) AS `__cg_key`
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      'LIKED' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'Post' AS `__cg_to_label`, 
      e.like_id AS `__cg_rid_0`, 
      NULL AS `__cg_rid_1`, 
      e.user_id AS `__cg_from_0`, 
      e.post_id AS `__cg_to_0`, 
      e.user_id AS `__cg_start_0`, 
      e.post_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('LIKED:User:Post:', CAST(e.like_id AS STRING)) AS `__cg_key`
FROM test_integration.post_likes_test AS e
)
SELECT 
      count(*) AS `total`
FROM test_integration.users_test AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
WHERE (v0.created_date > '2024-01-01' AND CASE WHEN regexp_replace(regexp_replace(typeof(v2.p2_v2_status__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v2.p2_v2_status__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v2.p2_v2_status__cg0, v2.p2_v2_status__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END = 'active')

-- column total: Value [("total", "total")]