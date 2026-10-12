//! The bound plan (P-4c, `docs/design/EXPLICIT_SCOPE.md` §4): queries whose
//! every variable is resolved once against an explicit scope.
//!
//! * [`types`]: bindings, scopes, the bound operators.
//! * [`binder`]: clause-list statement -> [`types::BoundStatement`].
//! * `expr`: expression binding (variables renamed to their bindings).
//! * `labels`: label inference over a clause's pattern.
//! * [`lower`]: bound plan -> `RenderPlan`, printed by
//!   `render_plan_to_sql_plain`, and the result shape Bolt / graph output
//!   read; what it does not lower is `LowerError::Unsupported`.
//!
//! Routing is in `crate::translate`: ClickHouse reads go through the bound
//! plan only (S11j); other dialects fall back to the legacy pipeline
//! for what it does not lower.

pub mod binder;
mod expr;
mod labels;
pub mod lower;
pub mod types;

#[cfg(test)]
mod tests;

pub use binder::bind_statement;
pub use types::*;
