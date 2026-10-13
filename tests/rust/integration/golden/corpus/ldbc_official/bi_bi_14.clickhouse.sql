WITH v10_both AS (
SELECT 
      e.Person1Id AS "Person1Id", 
      e.Person2Id AS "Person2Id", 
      e.creationDate AS "creationDate", 
      e.Person1Id AS "__cg_start_0", 
      e.Person2Id AS "__cg_end_0"
FROM ldbc.Person_knows_Person AS e

UNION ALL
SELECT 
      e.Person1Id AS "Person1Id", 
      e.Person2Id AS "Person2Id", 
      e.creationDate AS "creationDate", 
      e.Person2Id AS "__cg_start_0", 
      e.Person1Id AS "__cg_end_0"
FROM ldbc.Person_knows_Person AS e
WHERE e.Person1Id <> e.Person2Id
), 
with_w2 AS (
SELECT 
      v4.id AS "v11__id", 
      v4.id AS "p3_v11_id", 
      v9.id AS "v12__id", 
      v9.id AS "p3_v12_id", 
      v2.id AS "v13__id", 
      v2.name AS "p3_v13_name"
FROM ldbc.Place AS v0
JOIN ldbc.Place_isPartOf_Place AS v1 ON v1.Place2Id = v0.id
JOIN ldbc.Place AS v2 ON v1.Place1Id = v2.id
JOIN ldbc.Person_isLocatedIn_Place AS v3 ON v3.CityId = v2.id
JOIN ldbc.Person AS v4 ON v3.PersonId = v4.id
CROSS JOIN ldbc.Place AS v5
JOIN ldbc.Place_isPartOf_Place AS v6 ON v6.Place2Id = v5.id
JOIN ldbc.Place AS v7 ON v6.Place1Id = v7.id
JOIN ldbc.Person_isLocatedIn_Place AS v8 ON v8.CityId = v7.id
JOIN ldbc.Person AS v9 ON v8.PersonId = v9.id
JOIN v10_both AS v10 ON v10.__cg_start_0 = v4.id AND v10.__cg_end_0 = v9.id
WHERE (((v0.type = 'Country')) AND ((v2.type = 'City')) AND ((v5.type = 'Country')) AND ((v7.type = 'City')) AND v0.name = 'India' AND v5.name = 'China' AND (v1.Place1Id <> v6.Place1Id OR v1.Place2Id <> v6.Place2Id) AND (v3.PersonId <> v8.PersonId OR v3.CityId <> v8.CityId))
), 
optional_d3 AS (
SELECT DISTINCT 
      w2.v11__id AS "v11__id", 
      w2.v12__id AS "v12__id"
FROM with_w2 AS w2
), 
v18_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Post AS e
), 
v17_rels AS (
SELECT 
      'REPLY_OF' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Comment' AS "__cg_to_label", 
      e.Comment1Id AS "__cg_rid_0", 
      e.Comment2Id AS "__cg_rid_1", 
      e.Comment1Id AS "__cg_from_0", 
      e.Comment2Id AS "__cg_to_0", 
      e.Comment1Id AS "__cg_start_0", 
      e.Comment2Id AS "__cg_end_0", 
      'Comment' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('REPLY_OF:Comment:Comment:', toString(tuple(e.Comment1Id, e.Comment2Id))) AS "__cg_key"
FROM ldbc.Comment_replyOf_Comment AS e

UNION ALL
SELECT 
      'REPLY_OF' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.PostId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.PostId AS "__cg_to_0", 
      e.CommentId AS "__cg_start_0", 
      e.PostId AS "__cg_end_0", 
      'Comment' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('REPLY_OF:Comment:Post:', toString(tuple(e.CommentId, e.PostId))) AS "__cg_key"
FROM ldbc.Comment_replyOf_Post AS e
), 
v19_rels AS (
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.CommentId AS "__cg_start_0", 
      e.PersonId AS "__cg_end_0", 
      'Comment' AS "__cg_start_label", 
      'Person' AS "__cg_end_label", 
      concat('HAS_CREATOR:Comment:Person:', toString(tuple(e.CommentId, e.PersonId))) AS "__cg_key"
FROM ldbc.Comment_hasCreator_Person AS e

UNION ALL
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.PostId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.PostId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.PostId AS "__cg_start_0", 
      e.PersonId AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'Person' AS "__cg_end_label", 
      concat('HAS_CREATOR:Post:Person:', toString(tuple(e.PostId, e.PersonId))) AS "__cg_key"
FROM ldbc.Post_hasCreator_Person AS e
), 
optional_o4 AS (
SELECT 
      d3.v11__id AS "v11__id", 
      d3.v12__id AS "v12__id", 
      v16.id AS "v16__id"
FROM optional_d3 AS d3
JOIN ldbc.Comment_hasCreator_Person AS v15 ON v15.PersonId = d3.v11__id
JOIN ldbc.Comment AS v16 ON v15.CommentId = v16.id
JOIN v17_rels AS v17 ON v17.__cg_start_0 = v16.id
JOIN v18_labels AS v18 ON v17.__cg_end_label = v18.__cg_label AND v17.__cg_end_0 = v18.__cg_id_0
JOIN v19_rels AS v19 ON v19.__cg_start_label = v18.__cg_label AND v19.__cg_start_0 = v18.__cg_id_0 AND v19.__cg_end_0 = d3.v12__id
WHERE ('HAS_CREATOR' <> v19.__cg_type OR 'Comment' <> v19.__cg_from_label OR 'Person' <> v19.__cg_to_label OR v15.CommentId <> v19.__cg_rid_0 OR v15.PersonId <> v19.__cg_rid_1)
), 
with_w8 AS (
SELECT DISTINCT 
      w2.v11__id AS "v20__id", 
      w2.p3_v11_id AS "p3_v20_id", 
      w2.v12__id AS "v21__id", 
      w2.p3_v12_id AS "p3_v21_id", 
      w2.v13__id AS "v22__id", 
      w2.p3_v13_name AS "p3_v22_name", 
      0 + CASE WHEN o4.v16__id IS NULL THEN 0 ELSE 4 END AS "v23"
FROM with_w2 AS w2
LEFT JOIN optional_o4 AS o4 ON w2.v11__id = o4.v11__id AND w2.v12__id = o4.v12__id
), 
optional_d9 AS (
SELECT DISTINCT 
      w8.v20__id AS "v20__id", 
      w8.v21__id AS "v21__id"
FROM with_w8 AS w8
), 
v25_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Post AS e
), 
v24_rels AS (
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.PersonId AS "__cg_start_0", 
      e.CommentId AS "__cg_end_0", 
      'Person' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('HAS_CREATOR:Comment:Person:', toString(tuple(e.CommentId, e.PersonId))) AS "__cg_key"
FROM ldbc.Comment_hasCreator_Person AS e

UNION ALL
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.PostId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.PostId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.PersonId AS "__cg_start_0", 
      e.PostId AS "__cg_end_0", 
      'Person' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('HAS_CREATOR:Post:Person:', toString(tuple(e.PostId, e.PersonId))) AS "__cg_key"
FROM ldbc.Post_hasCreator_Person AS e
), 
v26_rels AS (
SELECT 
      'REPLY_OF' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Comment' AS "__cg_to_label", 
      e.Comment1Id AS "__cg_rid_0", 
      e.Comment2Id AS "__cg_rid_1", 
      e.Comment1Id AS "__cg_from_0", 
      e.Comment2Id AS "__cg_to_0", 
      e.Comment2Id AS "__cg_start_0", 
      e.Comment1Id AS "__cg_end_0", 
      'Comment' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('REPLY_OF:Comment:Comment:', toString(tuple(e.Comment1Id, e.Comment2Id))) AS "__cg_key"
FROM ldbc.Comment_replyOf_Comment AS e

UNION ALL
SELECT 
      'REPLY_OF' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.PostId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.PostId AS "__cg_to_0", 
      e.PostId AS "__cg_start_0", 
      e.CommentId AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('REPLY_OF:Comment:Post:', toString(tuple(e.CommentId, e.PostId))) AS "__cg_key"
FROM ldbc.Comment_replyOf_Post AS e
), 
optional_o10 AS (
SELECT 
      d9.v20__id AS "v20__id", 
      d9.v21__id AS "v21__id", 
      v25.__cg_label AS "v25____cg_label", 
      v25.__cg_id_0 AS "v25____cg_id_0"
FROM optional_d9 AS d9
JOIN v24_rels AS v24 ON v24.__cg_start_0 = d9.v20__id
JOIN v25_labels AS v25 ON v24.__cg_end_label = v25.__cg_label AND v24.__cg_end_0 = v25.__cg_id_0
JOIN v26_rels AS v26 ON v26.__cg_start_label = v25.__cg_label AND v26.__cg_start_0 = v25.__cg_id_0
JOIN ldbc.Comment AS v27 ON v26.__cg_end_0 = v27.id
JOIN ldbc.Comment_hasCreator_Person AS v28 ON v28.CommentId = v27.id AND v28.PersonId = d9.v21__id
WHERE (v24.__cg_type <> 'HAS_CREATOR' OR v24.__cg_from_label <> 'Comment' OR v24.__cg_to_label <> 'Person' OR v24.__cg_rid_0 <> v28.CommentId OR v24.__cg_rid_1 <> v28.PersonId)
), 
with_w14 AS (
SELECT DISTINCT 
      w8.v20__id AS "v29__id", 
      w8.p3_v20_id AS "p3_v29_id", 
      w8.v21__id AS "v30__id", 
      w8.p3_v21_id AS "p3_v30_id", 
      w8.v22__id AS "v31__id", 
      w8.p3_v22_name AS "p3_v31_name", 
      w8.v23 + CASE WHEN o10.v25____cg_label IS NULL THEN 0 ELSE 1 END AS "v32"
FROM with_w8 AS w8
LEFT JOIN optional_o10 AS o10 ON w8.v20__id = o10.v20__id AND w8.v21__id = o10.v21__id
), 
optional_d15 AS (
SELECT DISTINCT 
      w14.v29__id AS "v29__id", 
      w14.v30__id AS "v30__id"
FROM with_w14 AS w14
), 
v34_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Post AS e
), 
v33_rels AS (
SELECT 
      'LIKES' AS "__cg_type", 
      'Person' AS "__cg_from_label", 
      'Comment' AS "__cg_to_label", 
      e.PersonId AS "__cg_rid_0", 
      e.CommentId AS "__cg_rid_1", 
      e.PersonId AS "__cg_from_0", 
      e.CommentId AS "__cg_to_0", 
      e.PersonId AS "__cg_start_0", 
      e.CommentId AS "__cg_end_0", 
      'Person' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('LIKES:Person:Comment:', toString(tuple(e.PersonId, e.CommentId))) AS "__cg_key"
FROM ldbc.Person_likes_Comment AS e

UNION ALL
SELECT 
      'LIKES' AS "__cg_type", 
      'Person' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.PersonId AS "__cg_rid_0", 
      e.PostId AS "__cg_rid_1", 
      e.PersonId AS "__cg_from_0", 
      e.PostId AS "__cg_to_0", 
      e.PersonId AS "__cg_start_0", 
      e.PostId AS "__cg_end_0", 
      'Person' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('LIKES:Person:Post:', toString(tuple(e.PersonId, e.PostId))) AS "__cg_key"
FROM ldbc.Person_likes_Post AS e
), 
v35_rels AS (
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.CommentId AS "__cg_start_0", 
      e.PersonId AS "__cg_end_0", 
      'Comment' AS "__cg_start_label", 
      'Person' AS "__cg_end_label", 
      concat('HAS_CREATOR:Comment:Person:', toString(tuple(e.CommentId, e.PersonId))) AS "__cg_key"
FROM ldbc.Comment_hasCreator_Person AS e

UNION ALL
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.PostId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.PostId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.PostId AS "__cg_start_0", 
      e.PersonId AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'Person' AS "__cg_end_label", 
      concat('HAS_CREATOR:Post:Person:', toString(tuple(e.PostId, e.PersonId))) AS "__cg_key"
FROM ldbc.Post_hasCreator_Person AS e
), 
optional_o16 AS (
SELECT 
      d15.v29__id AS "v29__id", 
      d15.v30__id AS "v30__id", 
      v34.__cg_label AS "v34____cg_label", 
      v34.__cg_id_0 AS "v34____cg_id_0"
FROM optional_d15 AS d15
JOIN v33_rels AS v33 ON v33.__cg_start_0 = d15.v29__id
JOIN v34_labels AS v34 ON v33.__cg_end_label = v34.__cg_label AND v33.__cg_end_0 = v34.__cg_id_0
JOIN v35_rels AS v35 ON v35.__cg_start_label = v34.__cg_label AND v35.__cg_start_0 = v34.__cg_id_0 AND v35.__cg_end_0 = d15.v30__id
), 
with_w20 AS (
SELECT DISTINCT 
      w14.v29__id AS "v36__id", 
      w14.p3_v29_id AS "p3_v36_id", 
      w14.v30__id AS "v37__id", 
      w14.p3_v30_id AS "p3_v37_id", 
      w14.v31__id AS "v38__id", 
      w14.p3_v31_name AS "p3_v38_name", 
      w14.v32 + CASE WHEN o16.v34____cg_label IS NULL THEN 0 ELSE 10 END AS "v39"
FROM with_w14 AS w14
LEFT JOIN optional_o16 AS o16 ON w14.v29__id = o16.v29__id AND w14.v30__id = o16.v30__id
), 
optional_d21 AS (
SELECT DISTINCT 
      w20.v36__id AS "v36__id", 
      w20.v37__id AS "v37__id"
FROM with_w20 AS w20
), 
v41_labels AS (
SELECT 
      'Comment' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS "__cg_label", 
      e.id AS "__cg_id_0"
FROM ldbc.Post AS e
), 
v40_rels AS (
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Comment' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.CommentId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.CommentId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.PersonId AS "__cg_start_0", 
      e.CommentId AS "__cg_end_0", 
      'Person' AS "__cg_start_label", 
      'Comment' AS "__cg_end_label", 
      concat('HAS_CREATOR:Comment:Person:', toString(tuple(e.CommentId, e.PersonId))) AS "__cg_key"
FROM ldbc.Comment_hasCreator_Person AS e

UNION ALL
SELECT 
      'HAS_CREATOR' AS "__cg_type", 
      'Post' AS "__cg_from_label", 
      'Person' AS "__cg_to_label", 
      e.PostId AS "__cg_rid_0", 
      e.PersonId AS "__cg_rid_1", 
      e.PostId AS "__cg_from_0", 
      e.PersonId AS "__cg_to_0", 
      e.PersonId AS "__cg_start_0", 
      e.PostId AS "__cg_end_0", 
      'Person' AS "__cg_start_label", 
      'Post' AS "__cg_end_label", 
      concat('HAS_CREATOR:Post:Person:', toString(tuple(e.PostId, e.PersonId))) AS "__cg_key"
FROM ldbc.Post_hasCreator_Person AS e
), 
v42_rels AS (
SELECT 
      'LIKES' AS "__cg_type", 
      'Person' AS "__cg_from_label", 
      'Comment' AS "__cg_to_label", 
      e.PersonId AS "__cg_rid_0", 
      e.CommentId AS "__cg_rid_1", 
      e.PersonId AS "__cg_from_0", 
      e.CommentId AS "__cg_to_0", 
      e.CommentId AS "__cg_start_0", 
      e.PersonId AS "__cg_end_0", 
      'Comment' AS "__cg_start_label", 
      'Person' AS "__cg_end_label", 
      concat('LIKES:Person:Comment:', toString(tuple(e.PersonId, e.CommentId))) AS "__cg_key"
FROM ldbc.Person_likes_Comment AS e

UNION ALL
SELECT 
      'LIKES' AS "__cg_type", 
      'Person' AS "__cg_from_label", 
      'Post' AS "__cg_to_label", 
      e.PersonId AS "__cg_rid_0", 
      e.PostId AS "__cg_rid_1", 
      e.PersonId AS "__cg_from_0", 
      e.PostId AS "__cg_to_0", 
      e.PostId AS "__cg_start_0", 
      e.PersonId AS "__cg_end_0", 
      'Post' AS "__cg_start_label", 
      'Person' AS "__cg_end_label", 
      concat('LIKES:Person:Post:', toString(tuple(e.PersonId, e.PostId))) AS "__cg_key"
FROM ldbc.Person_likes_Post AS e
), 
optional_o22 AS (
SELECT 
      d21.v36__id AS "v36__id", 
      d21.v37__id AS "v37__id", 
      v41.__cg_label AS "v41____cg_label", 
      v41.__cg_id_0 AS "v41____cg_id_0"
FROM optional_d21 AS d21
JOIN v40_rels AS v40 ON v40.__cg_start_0 = d21.v36__id
JOIN v41_labels AS v41 ON v40.__cg_end_label = v41.__cg_label AND v40.__cg_end_0 = v41.__cg_id_0
JOIN v42_rels AS v42 ON v42.__cg_start_label = v41.__cg_label AND v42.__cg_start_0 = v41.__cg_id_0 AND v42.__cg_end_0 = d21.v37__id
), 
with_w26 AS (
SELECT DISTINCT 
      w20.v36__id AS "v43__id", 
      w20.p3_v36_id AS "p3_v43_id", 
      w20.v37__id AS "v44__id", 
      w20.p3_v37_id AS "p3_v44_id", 
      w20.v38__id AS "v45__id", 
      w20.p3_v38_name AS "p3_v45_name", 
      w20.v39 + CASE WHEN o22.v41____cg_label IS NULL THEN 0 ELSE 1 END AS "v46", 
      w20.p3_v38_name AS "__o0", 
      w20.v39 + CASE WHEN o22.v41____cg_label IS NULL THEN 0 ELSE 1 END AS "__o1", 
      w20.p3_v36_id AS "__o2", 
      w20.p3_v37_id AS "__o3"
FROM with_w20 AS w20
LEFT JOIN optional_o22 AS o22 ON w20.v36__id = o22.v36__id AND w20.v37__id = o22.v37__id
), 
with_w27 AS (
SELECT 
      w26.v43__id AS "v43__id", 
      w26.p3_v43_id AS "p3_v43_id", 
      w26.v44__id AS "v44__id", 
      w26.p3_v44_id AS "p3_v44_id", 
      w26.v45__id AS "v45__id", 
      w26.p3_v45_name AS "p3_v45_name", 
      w26.v46 AS "v46", 
      row_number() OVER (ORDER BY w26.__o0 ASC, w26.__o1 DESC NULLS FIRST, w26.__o2 ASC, w26.__o3 ASC) AS "__cg_row"
FROM with_w26 AS w26
), 
with_w28 AS (
SELECT 
      w27.v45__id AS "v47__id", 
      w27.p3_v45_name AS "p3_v47_name", 
      CASE WHEN length(arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf((w27.__cg_row, tuple(w27.v46, w27.p3_v43_id, w27.p3_v44_id)), true)))) > 0 THEN tupleElement(arrayElementOrNull(arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf((w27.__cg_row, tuple(w27.v46, w27.p3_v43_id, w27.p3_v44_id)), true))), 1), 1) ELSE NULL END AS "v48__k0", 
      CASE WHEN length(arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf((w27.__cg_row, tuple(w27.v46, w27.p3_v43_id, w27.p3_v44_id)), true)))) > 0 THEN tupleElement(arrayElementOrNull(arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf((w27.__cg_row, tuple(w27.v46, w27.p3_v43_id, w27.p3_v44_id)), true))), 1), 2) ELSE NULL END AS "v48__k1", 
      CASE WHEN length(arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf((w27.__cg_row, tuple(w27.v46, w27.p3_v43_id, w27.p3_v44_id)), true)))) > 0 THEN tupleElement(arrayElementOrNull(arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf((w27.__cg_row, tuple(w27.v46, w27.p3_v43_id, w27.p3_v44_id)), true))), 1), 3) ELSE NULL END AS "v48__k2"
FROM with_w27 AS w27
GROUP BY w27.v45__id, w27.p3_v45_name
)
SELECT 
      w28.v48__k1 AS "top.person1Id", 
      w28.v48__k2 AS "top.person2Id", 
      w28.p3_v47_name AS "city1.name", 
      w28.v48__k0 AS "top.score"
FROM with_w28 AS w28
ORDER BY w28.v48__k0 DESC NULLS FIRST, w28.v48__k1 ASC, w28.v48__k2 ASC
LIMIT 100