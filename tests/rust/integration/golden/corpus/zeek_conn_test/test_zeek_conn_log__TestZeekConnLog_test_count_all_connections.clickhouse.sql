SELECT 
      count(*) AS "total"
FROM test_zeek.conn_log AS v1
WHERE (v1.orig_h IS NOT NULL AND v1.resp_h IS NOT NULL)

-- column total: Value [("total", "total")]