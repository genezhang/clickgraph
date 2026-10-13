WITH with_w1 AS (
SELECT 
      v1.Dest AS "v3__code"
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
), 
with_w2 AS (
SELECT 
      v4.Dest AS "v6__code", 
      v4.Dest AS "p2_v6_code"
FROM with_w1 AS w1
JOIN test_integration.flights AS v4 ON v4.Origin = w1.v3__code
WHERE v4.Dest IS NOT NULL
)
SELECT 
      w2.p2_v6_code AS "x", 
      v7.Dest AS "y"
FROM with_w2 AS w2
JOIN test_integration.flights AS v7 ON v7.Origin = w2.v6__code
WHERE v7.Dest IS NOT NULL

-- column x: Value [("x", "x")]
-- column y: Value [("y", "y")]