//! Unit tests - Fast, isolated tests that don't require external dependencies
//!
//! These tests focus on individual components and functions without external I/O.
//! Most unit tests are embedded in source files with #[cfg(test)] modules.

// Include unit tests from source modules
// These are automatically discovered by cargo test when run with --lib

#[cfg(test)]
mod tests {
    // Unit tests are embedded in source files with #[cfg(test)]
    // This module serves as a placeholder for future standalone unit test files
}