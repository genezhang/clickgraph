SELECT 
      t0."id.orig_h" AS "src.ip", 
      t0."id.resp_h" AS "dest.ip"
FROM zeek.conn_log AS t0
