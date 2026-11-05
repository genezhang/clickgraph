SELECT 
      v1.follower_id AS `r.fromId`, 
      v1.followed_id AS `r.toId`, 
      v1.follow_date AS `r.follow_date`, 
      concat('FOLLOWS:', concat(CASE WHEN v1.follow_id IS NULL THEN '\\N' ELSE replace(replace(replace(CAST(v1.follow_id AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END), ':', CAST(v1.follower_id AS STRING), '->', CAST(v1.followed_id AS STRING), '-') AS `r.elementId`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
LIMIT 5
-- column r: Rel { rel_type: "FOLLOWS", from_label: "User", to_label: "User" } [("#fromId", "r.fromId"), ("#toId", "r.toId"), ("follow_date", "r.follow_date"), ("#elementId", "r.elementId")]