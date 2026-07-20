SELECT 
      c.commentId AS `c.commentId`, 
      reply.commentId AS `reply.commentId`, 
      reply.creatorId AS `p.personId`
FROM ldbc.comment AS c
INNER JOIN ldbc.comment AS reply ON reply.replyOfCommentId = c.commentId
WHERE c.commentId = 100
LIMIT 5