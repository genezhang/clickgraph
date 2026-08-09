SELECT 
      minOrNull(r.since) AS "earliest", 
      maxOrNull(r.since) AS "latest"
FROM test_integration.follows AS r
