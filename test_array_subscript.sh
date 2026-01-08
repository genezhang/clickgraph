#!/bin/bash

curl -s http://localhost:8080/query_sql \
  -H "Content-Type: application/json" \
  -d '{"query":"USE social_benchmark MATCH (u:User{user_id: 1}) RETURN labels(u)[1] as first_label"}' \
  | jq '.'
