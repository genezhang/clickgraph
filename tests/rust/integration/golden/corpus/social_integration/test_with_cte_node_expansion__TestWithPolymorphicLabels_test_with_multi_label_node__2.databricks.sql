WITH with_w1 AS (
SELECT 
      v0.post_id AS `v1__post_id`, 
      v0.author_id AS `p2_v1_author_id`, 
      v0.post_content AS `p2_v1_content`, 
      v0.post_date AS `p2_v1_created_at`, 
      v0.post_id AS `p2_v1_post_id`, 
      v0.post_title AS `p2_v1_title`
FROM test_integration.posts_test AS v0
)
SELECT 
      w1.p2_v1_author_id AS `p.author_id`, 
      w1.p2_v1_content AS `p.content`, 
      w1.p2_v1_created_at AS `p.created_at`, 
      w1.p2_v1_post_id AS `p.post_id`, 
      w1.p2_v1_title AS `p.title`
FROM with_w1 AS w1
LIMIT 1
-- column p: Node { label: "Post" } [("author_id", "p.author_id"), ("content", "p.content"), ("created_at", "p.created_at"), ("post_id", "p.post_id"), ("title", "p.title")]