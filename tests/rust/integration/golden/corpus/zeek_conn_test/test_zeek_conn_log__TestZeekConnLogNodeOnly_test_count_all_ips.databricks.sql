WITH __cg_nodes_IP__cg_rows AS (
SELECT 
      e.orig_h AS `ip`, 
      e.orig_p AS `port__cg0`, 
      NULL AS `port__cg1`
FROM test_zeek.conn_log AS e

UNION ALL
SELECT 
      e.resp_h AS `ip`, 
      NULL AS `port__cg0`, 
      e.resp_p AS `port__cg1`
FROM test_zeek.conn_log AS e
), 
__cg_nodes_IP AS (
SELECT 
      e.ip AS `ip`, 
      CASE WHEN count(DISTINCT CASE WHEN regexp_replace(regexp_replace(typeof(e.port__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.port__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.port__cg0, e.port__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END) > 1 THEN raise_error('ClickGraph: rows of one :IP node hold several values of its property port: the property is not the node\'s, or its id columns do not identify it') ELSE any_value(CASE WHEN regexp_replace(regexp_replace(typeof(e.port__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.port__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.port__cg0, e.port__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, true) END AS `port`
FROM __cg_nodes_IP__cg_rows AS e
WHERE NOT isnull(e.ip)
GROUP BY e.ip
)
SELECT 
      count(DISTINCT v0.ip) AS `cnt`
FROM __cg_nodes_IP AS v0

-- column cnt: Value [("cnt", "cnt")]