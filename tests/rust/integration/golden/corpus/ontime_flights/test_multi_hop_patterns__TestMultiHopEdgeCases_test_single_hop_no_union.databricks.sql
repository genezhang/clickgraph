SELECT `a.code` AS `a.code`, `b.code` AS `b.code` FROM (
SELECT 
      r.Origin AS `a.code`, 
      r.Dest AS `b.code`
FROM default.flights AS r
UNION ALL 
SELECT 
      r.Dest AS `a.code`, 
      r.Origin AS `b.code`
FROM default.flights AS r
) AS __union
LIMIT 5