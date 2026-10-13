use serde::{Deserialize, Serialize};

/// Represents a reference to a view or table in a render plan
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewTableRef {
    pub name: String,
    /// The alias to use in SQL (e.g., the Cypher variable name like "u" or "n")
    pub alias: Option<String>,
    /// Whether to use FINAL keyword for this table (for ReplacingMergeTree, etc.)
    pub use_final: bool,
}

impl ViewTableRef {
    /// `base_name(p = 'v', …)` for a parameterized view, else `base_name`.
    /// The lowering has the schema's parameter names and the request's
    /// values.
    pub fn parameterized_name(
        base_name: &str,
        param_names: Option<&[String]>,
        param_values: Option<&std::collections::HashMap<String, String>>,
    ) -> String {
        if let (Some(param_names), Some(param_values)) = (param_names, param_values) {
            if !param_names.is_empty() {
                // Generate parameterized view call with actual values
                // e.g., view_name(tenant_id='acme', region='US')
                let param_pairs: Vec<String> = param_names
                    .iter()
                    .filter_map(|name| {
                        param_values.get(name).map(|value| {
                            // Escape single quotes in value for SQL safety
                            let escaped_value = value.replace('\'', "''");
                            format!("{} = '{}'", name, escaped_value)
                        })
                    })
                    .collect();

                if param_pairs.is_empty() {
                    log::warn!(
                        "ViewTableRef: View '{}' expects parameters {:?} but none matched in provided values",
                        base_name, param_names
                    );
                    return base_name.to_string();
                }

                return format!("{}({})", base_name, param_pairs.join(", "));
            }
        }

        // Not a parameterized view - use plain table name
        base_name.to_string()
    }
}
