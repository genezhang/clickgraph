SELECT count(*) AS "n" FROM (
SELECT 
      a.Origin AS "a.code"
FROM test_integration.flights AS a
WHERE a.OriginState = 'CA'
UNION DISTINCT 
SELECT 
      a.Dest AS "a.code"
FROM test_integration.flights AS a
WHERE a.DestState = 'CA'
) AS __union
