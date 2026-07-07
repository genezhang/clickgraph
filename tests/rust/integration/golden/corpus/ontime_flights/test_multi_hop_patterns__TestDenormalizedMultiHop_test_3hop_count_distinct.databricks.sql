WITH with_destinations_hub_origins_cte_0 AS (SELECT 
      f2.Origin AS `hub`, 
      count(DISTINCT f1.Origin) AS `origins`, 
      count(DISTINCT f3.Dest) AS `destinations`
FROM default.flights AS f1
INNER JOIN default.flights AS f2 ON f2.Origin = f1.Dest
INNER JOIN default.flights AS f3 ON f3.Origin = f2.Dest
GROUP BY f2.Origin
)
SELECT 
      destinations_hub_origins.hub AS `hub`, 
      destinations_hub_origins.origins AS `origins`, 
      destinations_hub_origins.destinations AS `destinations`
FROM with_destinations_hub_origins_cte_0 AS destinations_hub_origins
ORDER BY destinations_hub_origins.origins DESC
LIMIT 5