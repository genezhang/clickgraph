WITH with_domain_source_ip_cte_1 AS (SELECT 
      dns."id.orig_h" AS "source_ip", 
      dns.query AS "domain"
FROM zeek.dns_log AS dns
)
SELECT DISTINCT 
      domain_source_ip.source_ip AS "source_ip", 
      domain_source_ip.domain AS "domain", 
      conn."id.resp_h" AS "dest_ip"
FROM zeek.conn_log AS conn
INNER JOIN with_domain_source_ip_cte_1 AS domain_source_ip ON conn."id.orig_h" = domain_source_ip.source_ip
WHERE conn."id.orig_h" = domain_source_ip.source_ip
ORDER BY domain_source_ip.source_ip ASC, domain_source_ip.domain ASC
