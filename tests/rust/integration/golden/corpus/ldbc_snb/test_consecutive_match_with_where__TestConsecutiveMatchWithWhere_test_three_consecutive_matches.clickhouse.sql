SELECT 
      v0.commentId AS "c.commentId", 
      v2.commentId AS "reply.commentId", 
      v4.personId AS "p.personId"
FROM ldbc.comment AS v0
JOIN ldbc.comment AS v1 ON v1.replyOfCommentId = v0.commentId
JOIN ldbc.comment AS v2 ON v1.commentId = v2.commentId
JOIN ldbc.comment AS v3 ON v3.commentId = v2.commentId
JOIN ldbc.person AS v4 ON v3.creatorId = v4.personId
WHERE v0.commentId = 100
LIMIT 5