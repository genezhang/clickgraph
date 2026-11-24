//! A list of literals of no common type (#1491).
//!
//! ClickHouse reads `[1, 'a']`, a list of literal tokens, as one constant
//! array and finds no common type for it (Code 386), whatever
//! `use_variant_as_common_type` says; `array(1, 'a')`, a function call, makes
//! a `Variant`. Every place that writes a list of literals into SQL (the
//! lowering of a list, the substitution of a list parameter) spells such a
//! list as the call.

/// What a literal list element is, as far as the constant-array reader tells:
/// what two of them can meet in.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LiteralType {
    /// A `NULL`, and the elements of `[]`: meets any type.
    Any,
    /// A number or a boolean (`UInt8`, `Int8`, `Float64`, ...: they meet).
    Number,
    String,
    /// A list of literals, of its elements' meeting type.
    List(Box<LiteralType>),
}

impl LiteralType {
    /// The type two elements meet in, `None` where ClickHouse has no common
    /// type for them (a string and a number, a list and a number, a list of
    /// strings and a list of numbers).
    pub(crate) fn meet(&self, other: &LiteralType) -> Option<LiteralType> {
        use LiteralType::*;
        match (self, other) {
            (Any, t) | (t, Any) => Some(t.clone()),
            (Number, Number) => Some(Number),
            (String, String) => Some(String),
            (List(a), List(b)) => a.meet(b).map(|t| List(Box::new(t))),
            _ => None,
        }
    }
}

/// The type the `elements` meet in: `None` if two of them have no common
/// type, else the meeting type. `kind` reads an element's type (`None`: not
/// a literal, which makes the whole list not a list of literals: the caller
/// handles it before, so this returns `Some(None)` only for a mix).
pub(crate) fn meeting_type<T>(
    elements: &[T],
    kind: &impl Fn(&T) -> Option<Option<LiteralType>>,
) -> Option<Option<LiteralType>> {
    // Outer `None`: not all literals. Inner `None`: no common type.
    let mut meeting = Some(LiteralType::Any);
    for e in elements {
        let t = kind(e)?;
        meeting = match (meeting, t) {
            (Some(m), Some(t)) => m.meet(&t),
            _ => None,
        };
    }
    Some(meeting)
}

/// Whether `elements` are all literals (or lists of them) of no common type:
/// the list ClickHouse reads as one constant array and refuses.
pub(crate) fn of_no_common_type<T>(
    elements: &[T],
    kind: &impl Fn(&T) -> Option<Option<LiteralType>>,
) -> bool {
    matches!(meeting_type(elements, kind), Some(None))
}
