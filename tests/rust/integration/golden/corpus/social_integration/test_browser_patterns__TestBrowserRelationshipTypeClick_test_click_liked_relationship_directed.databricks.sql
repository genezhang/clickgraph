SELECT 
      v1.user_id AS `r.from_id`, 
      v1.post_id AS `r.to_id`, 
      v1.like_date AS `r.like_date`, 
      concat('LIKED:', concat(CASE WHEN v1.like_id IS NULL THEN '\\N' ELSE replace(replace(replace(CAST(v1.like_id AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END), ':', CAST(v1.user_id AS STRING), '->', CAST(v1.post_id AS STRING), '-') AS `r.elementId`
FROM test_integration.users_test AS v0
JOIN test_integration.post_likes_test AS v1 ON v1.user_id = v0.user_id
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
LIMIT 25
-- column r: Rel { rel_type: "LIKED", from_label: "User", to_label: "Post" } [("from_id", "r.from_id"), ("to_id", "r.to_id"), ("like_date", "r.like_date"), ("#elementId", "r.elementId")]