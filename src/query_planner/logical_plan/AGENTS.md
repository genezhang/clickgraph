# logical_plan — the legacy plan types

Only the types remain (P-4c S14c3 deleted the clause builders and the
planning entry points). `LogicalPlan` is still named by shared structures —
`ViewTableRef.source`, `LogicalExpr::{ExistsSubquery, InSubquery}`,
`CteGenerationContext`, PageRank's `CALL` — and `VariableLengthSpec` /
`ShortestPathMode` are read by the path generator. S14c6 decouples those and
deletes the rest (`docs/design/EXPLICIT_SCOPE.md` S14).
