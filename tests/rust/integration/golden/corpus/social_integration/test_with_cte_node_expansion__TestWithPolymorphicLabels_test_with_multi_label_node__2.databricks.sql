WITH with_p_cte_0 AS (SELECT 
      p.author_id AS `p1_p_author_id`, 
      p.post_content AS `p1_p_content`, 
      p.post_date AS `p1_p_created_at`, 
      p.post_id AS `p1_p_post_id`, 
      p.post_title AS `p1_p_title`
FROM test_integration.posts_test AS p
)
SELECT 
      p.p1_p_author_id AS `p.author_id`, 
      p.p1_p_content AS `p.content`, 
      p.p1_p_created_at AS `p.created_at`, 
      p.p1_p_content AS `p.post_content`, 
      p.p1_p_created_at AS `p.post_date`, 
      p.p1_p_post_id AS `p.post_id`, 
      p.p1_p_title AS `p.post_title`, 
      p.p1_p_title AS `p.title`
FROM with_p_cte_0 AS p
LIMIT 1