SELECT 
      transform(filter(element_at(transform(array(named_struct('s', 1, 'e', 5, 't', 1)), __cg_r -> CASE WHEN (__cg_r.t > 0 AND __cg_r.s > __cg_r.e) OR (__cg_r.t < 0 AND __cg_r.s < __cg_r.e) THEN array() ELSE sequence(__cg_r.s, __cg_r.e, __cg_r.t) END), 1), x -> x % 2 = 0), x -> x * 10) AS `c`
