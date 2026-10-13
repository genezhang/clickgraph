SELECT 
      NULL AS "a.created_at", 
      NULL AS "c.created_at"
WHERE false
LIMIT 5
-- column a.created_at: Value [("a.created_at", "a.created_at")]
-- column c.created_at: Value [("c.created_at", "c.created_at")]