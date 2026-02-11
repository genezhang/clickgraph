//! String Arena for AST Transformation
//!
//! Provides memory-safe string allocation for AST transformations without leaking.
//! The arena is dropped after query planning, freeing all allocated strings.

use bumpalo::Bump;

/// Arena allocator for AST string references
///
/// Replaces `Box::leak()` pattern with proper memory management.
/// All strings allocated via this arena live as long as the arena itself,
/// and are freed when the arena is dropped.
pub struct StringArena {
    bump: Bump,
}

impl StringArena {
    /// Create a new string arena
    pub fn new() -> Self {
        Self { bump: Bump::new() }
    }

    /// Allocate a string slice with lifetime tied to the arena
    ///
    /// # Example
    /// ```ignore
    /// let arena = StringArena::new();
    /// let s: &str = arena.alloc_str("hello");
    /// // s is valid as long as arena exists
    /// // When arena is dropped, s's memory is freed
    /// ```
    pub fn alloc_str<'a>(&'a self, s: &str) -> &'a str {
        self.bump.alloc_str(s)
    }

    /// Allocate a formatted string
    pub fn alloc_format(&self, s: String) -> &str {
        self.bump.alloc_str(&s)
    }
}

impl Default for StringArena {
    fn default() -> Self {
        Self::new()
    }
}
