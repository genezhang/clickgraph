SELECT 
      v1.Origin AS `a.code`, 
      v1.Dest AS `b.code`
FROM default.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
LIMIT 10
-- column a.code: Value [("a.code", "a.code")]
-- column b.code: Value [("b.code", "b.code")]