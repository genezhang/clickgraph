SELECT 
      NULL AS "a.content", 
      NULL AS "b.content"
WHERE false
LIMIT 10
-- column a.content: Value [("a.content", "a.content")]
-- column b.content: Value [("b.content", "b.content")]