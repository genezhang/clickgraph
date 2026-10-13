SELECT 
      'AUTHORED' AS "type(r)", 
      v0.user_id AS "id(a)", 
      ['User'] AS "labels(a)"
FROM test_integration.users_test AS v0
JOIN test_integration.posts_test AS v1 ON v1.author_id = v0.user_id
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
LIMIT 5
-- column type(r): Value [("type(r)", "type(r)")]
-- column id(a): NodeId { label: "User" } [("id(a)", "id(a)")]
-- column labels(a): Value [("labels(a)", "labels(a)")]