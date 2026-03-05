//! `QueryResult` and `Row` types — the output of `Connection::query()`.

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
}

impl QueryResult {
    pub(crate) fn new(column_names: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            column_names,
            rows,
            position: 0,
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
    fn test_empty_result() {
        let r = QueryResult::new(vec!["col".to_string()], vec![]);
        assert!(r.is_empty());
        assert_eq!(r.num_rows(), 0);
        let rows: Vec<Row> = r.collect();
        assert!(rows.is_empty());
    }
}
