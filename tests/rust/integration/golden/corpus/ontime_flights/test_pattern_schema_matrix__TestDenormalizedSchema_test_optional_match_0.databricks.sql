WITH __denorm_scan_a AS (
SELECT 
      a.OriginCityName AS `city`, 
      a.Origin AS `code`, 
      a.OriginState AS `state`
FROM default.flights AS a
UNION DISTINCT 
SELECT 
      a.DestCityName AS `city`, 
      a.Dest AS `code`, 
      a.DestState AS `state`
FROM default.flights AS a

)
SELECT 
      a.code AS `a.code`, 
      count(r.flight_id) AS `rel_count`
FROM __denorm_scan_a AS a
LEFT JOIN default.flights AS r ON a.code = r.Origin
GROUP BY a.code
