SELECT 
      element_at(transform(array(named_struct('s', 1, 'e', 10, 't', 2)), __cg_r -> CASE WHEN (__cg_r.t > 0 AND __cg_r.s > __cg_r.e) OR (__cg_r.t < 0 AND __cg_r.s < __cg_r.e) THEN array() ELSE sequence(__cg_r.s, __cg_r.e, __cg_r.t) END), 1) AS `asc`, 
      element_at(transform(array(named_struct('s', 5, 'e', 1, 't', 0 - 1)), __cg_r -> CASE WHEN (__cg_r.t > 0 AND __cg_r.s > __cg_r.e) OR (__cg_r.t < 0 AND __cg_r.s < __cg_r.e) THEN array() ELSE sequence(__cg_r.s, __cg_r.e, __cg_r.t) END), 1) AS `desc`
FROM social.users_bench AS u
