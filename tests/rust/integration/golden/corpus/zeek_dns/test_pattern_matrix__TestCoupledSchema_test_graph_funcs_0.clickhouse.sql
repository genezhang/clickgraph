SELECT 
      'REQUESTED' AS "type(r)", 
      v1."id.orig_h" AS "id(a)", 
      ['IP'] AS "labels(a)"
FROM zeek.dns_log AS v1
WHERE (v1."id.orig_h" IS NOT NULL AND v1.query IS NOT NULL)
LIMIT 5
-- column type(r): Value [("type(r)", "type(r)")]
-- column id(a): NodeId { label: "IP" } [("id(a)", "id(a)")]
-- column labels(a): Value [("labels(a)", "labels(a)")]