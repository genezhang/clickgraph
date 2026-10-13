SELECT 
      v2.post_id AS "p.post_id", 
      v2.content AS "p.content"
FROM brahmand.users_bench AS v0
JOIN brahmand.interactions AS v1 ON v1.from_id = v0.user_id
JOIN brahmand.posts_bench AS v2 ON v1.to_id = v2.post_id
WHERE (((((v1."interaction_type" = 'LIKES') AND (v1."from_type" = 'User')) AND (v1."to_type" = 'Post'))) AND v0.user_id = 1)
LIMIT 10
-- column p.post_id: Value [("p.post_id", "p.post_id")]
-- column p.content: Value [("p.content", "p.content")]