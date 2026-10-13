SELECT 
      count(*) AS "total"
FROM zeek.conn_log AS v1
WHERE (v1."id.orig_h" IS NOT NULL AND v1."id.resp_h" IS NOT NULL)

-- column total: Value [("total", "total")]