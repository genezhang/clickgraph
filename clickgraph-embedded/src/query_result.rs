//! `QueryResult` and `Row` types — the output of `Connection::query()`.

use std::collections::HashMap;
use std::ops::Index;

use super::value::Value;

/// The result of a Cypher query.
///
/// Implements `Iterator<Item = Row>` for row-by-row processing.
///
/// Mirrors `kuzu::QueryResult`.
#[derive(Debug)]
pub struct QueryResult {
    column_names: Vec<String>,
    rows: Vec<Vec<Value>>,
    position: usize,
    /// Time spent translating Cypher to SQL (milliseconds).
    compile_time_ms: f64,
    /// Time spent executing the SQL query (milliseconds).
    execution_time_ms: f64,
    /// Side-effect counters for write+RETURN queries (Phase 5d).
    /// `Some` when the originating Cypher contained a write clause
    /// (CREATE / SET / DELETE / REMOVE) *and* a RETURN clause; the row
    /// payload then carries the user-visible result of the read pipeline,
    /// while these counters reflect the write portion.
    /// `None` for pure read queries and (for back-compat) for pure-write
    /// queries that surface counters as a synthetic single-row payload.
    write_counters: Option<HashMap<String, i64>>,
}

impl QueryResult {
    pub(crate) fn new(column_names: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            column_names,
            rows,
            position: 0,
            compile_time_ms: 0.0,
            execution_time_ms: 0.0,
            write_counters: None,
        }
    }

    pub(crate) fn with_timing(
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
        compile_time_ms: f64,
        execution_time_ms: f64,
    ) -> Self {
        Self {
            column_names,
            rows,
            position: 0,
            compile_time_ms,
            execution_time_ms,
            write_counters: None,
        }
    }

    /// Construct a write+RETURN result: user-visible rows plus the
    /// side-effect counter map produced by the write portion of the
    /// statement. Used by `handle_write_async` to attach write counters
    /// to a result whose row payload comes from re-running the read
    /// pipeline after the writes have executed.
    pub(crate) fn with_timing_and_counters(
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
        compile_time_ms: f64,
        execution_time_ms: f64,
        write_counters: HashMap<String, i64>,
    ) -> Self {
        Self {
            column_names,
            rows,
            position: 0,
            compile_time_ms,
            execution_time_ms,
            write_counters: Some(write_counters),
        }
    }

    /// Return the ordered list of column names in this result.
    pub fn get_column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Return the total number of rows.
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Return true if there are no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Time spent translating Cypher to SQL (milliseconds).
    ///
    /// Mirrors `kuzu::QueryResult::get_compiling_time()`.
    pub fn get_compiling_time(&self) -> f64 {
        self.compile_time_ms
    }

    /// Time spent executing the SQL query (milliseconds).
    ///
    /// Mirrors `kuzu::QueryResult::get_execution_time()`.
    pub fn get_execution_time(&self) -> f64 {
        self.execution_time_ms
    }

    /// Side-effect counters for a write+RETURN query (Phase 5d).
    ///
    /// Returns `Some` only when the originating Cypher contained both a
    /// write clause (CREATE / SET / DELETE / REMOVE) and a RETURN clause —
    /// the row payload then carries the read-pipeline output and these
    /// counters reflect the write portion. Pure read queries and
    /// pure-write queries (which surface counters as a synthetic row)
    /// return `None`.
    pub fn get_write_counters(&self) -> Option<&HashMap<String, i64>> {
        self.write_counters.as_ref()
    }

    /// Infer column data types from the first row of results.
    ///
    /// Returns a type name string per column: `"Null"`, `"Bool"`, `"Int64"`,
    /// `"Float64"`, `"String"`, `"List"`, or `"Map"`. Returns `"Null"` for
    /// empty results or columns where the first row has a null value.
    ///
    /// Mirrors `kuzu::QueryResult::get_column_data_types()`.
    pub fn get_column_data_types(&self) -> Vec<String> {
        if self.rows.is_empty() {
            return self
                .column_names
                .iter()
                .map(|_| "Null".to_string())
                .collect();
        }
        self.rows[0]
            .iter()
            .map(|v| v.type_name().to_string())
            .collect()
    }
}

impl Iterator for QueryResult {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.rows.len() {
            return None;
        }
        let values = self.rows[self.position].clone();
        self.position += 1;
        Some(Row {
            column_names: self.column_names.clone(),
            values,
        })
    }
}

/// A single row from a query result.
///
/// Supports both index access (`row[0]`) and column-name access (`row.get("name")`).
///
/// Mirrors `kuzu::FlatTuple`.
#[derive(Debug, Clone)]
pub struct Row {
    column_names: Vec<String>,
    values: Vec<Value>,
}

impl Row {
    /// Get a value by column name.  Returns `None` if the column does not exist.
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.column_names
            .iter()
            .position(|c| c == column)
            .and_then(|i| self.values.get(i))
    }

    /// Return all values in this row, in column order.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Return the column names for this row.
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }
}

impl Index<usize> for Row {
    type Output = Value;

    fn index(&self, idx: usize) -> &Value {
        &self.values[idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn make_result() -> QueryResult {
        QueryResult::new(
            vec!["name".to_string(), "age".to_string()],
            vec![
                vec![Value::String("Alice".to_string()), Value::Int64(30)],
                vec![Value::String("Bob".to_string()), Value::Int64(25)],
            ],
        )
    }

    #[test]
    fn test_num_rows() {
        let r = make_result();
        assert_eq!(r.num_rows(), 2);
        assert!(!r.is_empty());
    }

    #[test]
    fn test_column_names() {
        let r = make_result();
        assert_eq!(r.get_column_names(), &["name", "age"]);
    }

    #[test]
    fn test_iterator() {
        let r = make_result();
        let rows: Vec<Row> = r.collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::String("Alice".to_string()));
        assert_eq!(rows[1][0], Value::String("Bob".to_string()));
    }

    #[test]
    fn test_row_index_access() {
        let r = make_result();
        let mut iter = r;
        let row = iter.next().unwrap();
        assert_eq!(row[0], Value::String("Alice".to_string()));
        assert_eq!(row[1], Value::Int64(30));
    }

    #[test]
    fn test_row_get_by_name() {
        let r = make_result();
        let row = r.into_iter().next().unwrap();
        assert_eq!(row.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(row.get("age"), Some(&Value::Int64(30)));
        assert_eq!(row.get("missing"), None);
    }

    #[test]
    fn test_timing_defaults() {
        let r = make_result();
        assert_eq!(r.get_compiling_time(), 0.0);
        assert_eq!(r.get_execution_time(), 0.0);
    }

    #[test]
    fn test_timing_with_values() {
        let r =
            QueryResult::with_timing(vec!["x".to_string()], vec![vec![Value::Int64(1)]], 1.5, 2.5);
        assert_eq!(r.get_compiling_time(), 1.5);
        assert_eq!(r.get_execution_time(), 2.5);
    }

    #[test]
    fn test_column_data_types() {
        let r = QueryResult::new(
            vec!["name".to_string(), "age".to_string(), "active".to_string()],
            vec![vec![
                Value::String("Alice".to_string()),
                Value::Int64(30),
                Value::Bool(true),
            ]],
        );
        assert_eq!(r.get_column_data_types(), vec!["String", "Int64", "Bool"]);
    }

    #[test]
    fn test_column_data_types_empty() {
        let r = QueryResult::new(vec!["a".to_string(), "b".to_string()], vec![]);
        assert_eq!(r.get_column_data_types(), vec!["Null", "Null"]);
    }

    #[test]
    fn test_empty_result() {
        let r = QueryResult::new(vec!["col".to_string()], vec![]);
        assert!(r.is_empty());
        assert_eq!(r.num_rows(), 0);
        let rows: Vec<Row> = r.collect();
        assert!(rows.is_empty());
    }
}
