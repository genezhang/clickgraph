# TODO: Fix Parameterized Views SQL Generation

**Status**: Feature 95% complete, SQL generation bug discovered in e2e testing  
**Date**: November 15, 2025

## What Works ✅
- Schema configuration with `view_parameters` field (Task 7) - DONE
- Bolt protocol parameter extraction (Task 9) - DONE  
- HTTP API parameter passing - DONE
- Unit tests (7/7 passing) - DONE
- ClickHouse parameterized views created and manually tested - DONE
- Server starts correctly with proper environment variables - DONE
- Schema loads successfully - DONE

## The Bug 🐛

**Symptom**: E2E test shows queries fail with:
```
Code: 47. DB::Exception: Identifier 'u.user_id' cannot be resolved from table with name u
```

**Root Cause**: SQL generator creates:
```sql
SELECT ... FROM brahmand.users_by_tenant AS u
```

Should create:
```sql
SELECT ... FROM brahmand.users_by_tenant(tenant_id = 'acme') AS u
```

**Location**: SQL generation in `src/clickhouse_query_generator/view_scan.rs`

## What's Happening

1. ✅ Schema loads with `view_parameters: [tenant_id]`
2. ✅ HTTP request passes `view_parameters: {"tenant_id": "acme"}`
3. ✅ Parameters reach query handler
4. ❌ **ViewScan doesn't get view_parameter_names/values populated**
5. ❌ SQL generator fallsback to plain table name

## The Issue

From Task 8 implementation (`view_scan.rs`):
```rust
pub fn build_view_scan_table_ref(scan: &ViewScan) -> String {
    let table_ref = if let (Some(param_names), Some(param_values)) = 
        (&scan.view_parameter_names, &scan.view_parameter_values) 
    {
        // Generate: view_name(param='value')
        let param_pairs: Vec<String> = param_names.iter()
            .filter_map(|name| {
                param_values.get(name).map(|value| {
                    let escaped_value = value.replace('\'', "''");
                    format!("{} = '{}'", name, escaped_value)
                })
            })
            .collect();
        
        if !param_pairs.is_empty() {
            format!("{}({})", scan.source_table, param_pairs.join(", "))
        } else {
            scan.source_table.clone() // ← HITTING THIS PATH
        }
    } else {
        scan.source_table.clone() // ← OR THIS PATH
    };
    // ...
}
```

**The code is correct**, but `ViewScan` isn't being populated with parameters.

## Where to Look Tomorrow

**Priority 1**: Check parameter flow in query planner
- `src/query_planner/logical_plan/match_clause.rs` - Where ViewScan is created
- Does it collect `view_parameters` from schema?
- Does it get `view_parameter_values` from PlanCtx?

**Priority 2**: Check PlanCtx threading
- `src/query_planner/plan_ctx/mod.rs` - Check if `view_parameter_values` is set
- `src/server/handlers.rs` - Check if parameters passed to query planner

**Priority 3**: Add debug logging
```rust
// In view_scan.rs
eprintln!("DEBUG ViewScan: param_names={:?}, param_values={:?}", 
          scan.view_parameter_names, scan.view_parameter_values);
```

## Test Commands

```powershell
# Kill any existing server
Get-NetTCPConnection -LocalPort 8080 -ErrorAction SilentlyContinue | 
  Select-Object -ExpandProperty OwningProcess | 
  ForEach-Object { Stop-Process -Id $_ -Force }

# Run full e2e test
.\scripts\test\run_parameterized_views_test.ps1

# Or run server + test separately
.\scripts\server\start_server_background.ps1 `
  -ConfigPath "schemas/test/multi_tenant.yaml" `
  -Database "brahmand" `
  -ClickHouseUser "test_user" `
  -ClickHousePassword "test_pass" `
  -DisableBolt

python tests/integration/test_parameterized_views_http.py
```

## Expected Fix

Likely a small fix in `match_clause.rs` to populate ViewScan fields:
```rust
// Somewhere in ViewScan creation
let view_scan = ViewScan {
    source_table: table_name.clone(),
    view_parameter_names: Some(schema.view_parameters.clone()), // ← Add this
    view_parameter_values: plan_ctx.view_parameter_values.clone(), // ← Add this
    // ... rest of fields
};
```

## Commits So Far

- 2d1cb04: Task 7 - Schema configuration
- 7ea4a05: Task 8 - SQL generation  
- 4ad7563: Task 9 - Bolt protocol
- 8c21fca: Task 10 - Test infrastructure
- a639049: Task 11 - Unit tests
- fa215e3: Task 12 - Documentation + cleanup

## Next Session

1. Add debug logging to trace parameter flow
2. Fix ViewScan population in match_clause.rs
3. Verify e2e tests pass
4. Commit final fix
5. **Phase 2 Parameterized Views COMPLETE!** 🎉

---

**Note**: The core architecture is solid. This is just a data flow issue - parameters aren't making it from HTTP request → PlanCtx → ViewScan → SQL generation. Probably a 15-30 minute fix once we trace the flow.
