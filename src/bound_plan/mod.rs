//! The bound plan (P-4c, `docs/design/EXPLICIT_SCOPE.md` §4): queries whose
//! every variable is resolved once against an explicit scope.
//!
//! * [`types`]: bindings, scopes, the bound operators.
//! * [`binder`]: clause-list statement -> [`types::BoundStatement`].
//! * `expr`: expression binding (variables renamed to their bindings).
//! * `labels`: label inference over a clause's pattern.
//!
//! S3 binds; nothing here generates SQL yet (lowering is S4).

pub mod binder;
mod expr;
mod labels;
pub mod types;

#[cfg(test)]
mod tests;

pub use binder::bind_statement;
pub use types::*;
