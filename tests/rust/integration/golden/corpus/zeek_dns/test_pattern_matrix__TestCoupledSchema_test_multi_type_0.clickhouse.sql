SELECT 
      NULL AS "type(r)", 
      count(*) AS "cnt"
WHERE false
HAVING count(*) > 0

-- column type(r): Value [("type(r)", "type(r)")]
-- column cnt: Value [("cnt", "cnt")]