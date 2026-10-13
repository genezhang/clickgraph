SELECT 
      NULL AS "userId", 
      CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS "postCount", 
      CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS "likeCount"
WHERE false
HAVING count(*) > 0
ORDER BY CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END DESC NULLS FIRST
LIMIT 5
-- column userId: Value [("userId", "userId")]
-- column postCount: Value [("postCount", "postCount")]
-- column likeCount: Value [("likeCount", "likeCount")]