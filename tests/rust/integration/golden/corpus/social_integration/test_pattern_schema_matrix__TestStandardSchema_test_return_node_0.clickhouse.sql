SELECT 
      n.author_id AS "n.author_id", 
      n.post_content AS "n.content", 
      n.post_date AS "n.created_at", 
      n.post_id AS "n.post_id", 
      n.post_title AS "n.title"
FROM test_integration.posts_test AS n
LIMIT 5