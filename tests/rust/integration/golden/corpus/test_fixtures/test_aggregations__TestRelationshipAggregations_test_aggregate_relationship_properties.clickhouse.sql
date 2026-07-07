SELECT 
      min(r.since) AS "earliest", 
      max(r.since) AS "latest"
FROM test_integration.follows AS r
