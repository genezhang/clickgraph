SELECT 
      r.query AS "r.from_id", 
      r.answers AS "r.to_id", 
      r.query AS "r.query", 
      r.ts AS "r.timestamp", 
      r.uid AS "r.uid"
FROM zeek.dns_log AS r
LIMIT 5