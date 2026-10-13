SELECT 
      v1.`id.orig_h` AS `src.ip`, 
      v1.query AS `d.name`, 
      v1.answers AS `r.answers`
FROM zeek.dns_log AS v1
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.query IS NOT NULL AND v1.query = 'cdn.example.com')

-- column src.ip: Value [("src.ip", "src.ip")]
-- column d.name: Value [("d.name", "d.name")]
-- column r.answers: Value [("r.answers", "r.answers")]