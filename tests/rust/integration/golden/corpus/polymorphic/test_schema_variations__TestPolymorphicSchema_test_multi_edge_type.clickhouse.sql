WITH v2_labels AS (
SELECT 
      'Post' AS "__cg_label", 
      e.post_id AS "__cg_id_0", 
      e.content AS "p2_v2_content", 
      e.created_at AS "p2_v2_created", 
      NULL AS "p2_v2_email", 
      NULL AS "p2_v2_name", 
      e.post_id AS "p2_v2_post_id", 
      e.content AS "p2_v2_title", 
      NULL AS "p2_v2_user_id"
FROM brahmand.posts_bench AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      NULL AS "p2_v2_content", 
      NULL AS "p2_v2_created", 
      e.email_address AS "p2_v2_email", 
      e.full_name AS "p2_v2_name", 
      NULL AS "p2_v2_post_id", 
      NULL AS "p2_v2_title", 
      e.user_id AS "p2_v2_user_id"
FROM brahmand.users_bench AS e
), 
v1_rels AS (
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('FOLLOWS:User:Post:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'User')) AND (e."to_type" = 'Post')))

UNION ALL
SELECT 
      'FOLLOWS' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'User' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'User' AS "__cg_end_label", 
      concat('FOLLOWS:User:User:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'FOLLOWS') AND (e."from_type" = 'User')) AND (e."to_type" = 'User')))

UNION ALL
SELECT 
      'LIKES' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('LIKES:User:Post:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'LIKES') AND (e."from_type" = 'User')) AND (e."to_type" = 'Post')))

UNION ALL
SELECT 
      'LIKES' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'User' AS "__cg_to_label", 
      e.from_id AS "__cg_rid_0", 
      e.to_id AS "__cg_rid_1", 
      e.interaction_type AS "__cg_rid_2", 
      e.timestamp AS "__cg_rid_3", 
      e.from_id AS "__cg_from_0", 
      e.to_id AS "__cg_to_0", 
      e.from_id AS "__cg_start_0", 
      e.to_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'User' AS "__cg_end_label", 
      concat('LIKES:User:User:', toString(tuple(e.from_id, e.to_id, e.interaction_type, e.timestamp))) AS "__cg_key"
FROM brahmand.interactions AS e
WHERE ((((e."interaction_type" = 'LIKES') AND (e."from_type" = 'User')) AND (e."to_type" = 'User')))
)
SELECT 
      map('elementId', CAST(concat(v2.__cg_label, ':', toString(v2.__cg_id_0), '-'), 'Dynamic'), 'labels', CAST([v2.__cg_label], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('content', CAST(v2.p2_v2_content, 'Dynamic'), 'created', CAST(v2.p2_v2_created, 'Dynamic'), 'email', CAST(v2.p2_v2_email, 'Dynamic'), 'name', CAST(v2.p2_v2_name, 'Dynamic'), 'post_id', CAST(v2.p2_v2_post_id, 'Dynamic'), 'title', CAST(v2.p2_v2_title, 'Dynamic'), 'user_id', CAST(v2.p2_v2_user_id, 'Dynamic'))), 'Dynamic')) AS "target"
FROM brahmand.users_bench AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
WHERE v0.user_id = 1
LIMIT 10
-- column target: Graph(Node) [("target", "target")]