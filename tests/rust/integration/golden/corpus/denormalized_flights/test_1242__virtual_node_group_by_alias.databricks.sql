SELECT `s` AS `s`, count(*) AS `n` FROM (
SELECT 
      a.OriginState AS `a.state`,
      a.Origin AS `a.code`,
      a.OriginState AS `s`
FROM test_integration.flights AS a
UNION DISTINCT 
SELECT 
      a.DestState AS `a.state`,
      a.Dest AS `a.code`,
      a.DestState AS `s`
FROM test_integration.flights AS a
) AS __union
GROUP BY `s`
