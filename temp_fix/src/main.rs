use std::fs;
use std::path::Path;
use walkdir::WalkDir;

fn fix_relationship_pattern_file(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let content = fs::read_to_string(file_path)?;
    
    if !content.contains("RelationshipPattern {") {
        return Ok(()); // No RelationshipPattern in this file
    }
    
    // Add the import first if it's in the parser module
    let mut updated_content = content.clone();
    
    if file_path.to_string_lossy().contains("open_cypher_parser") && 
       content.contains("use super::ast::{") && 
       !content.contains("VariableLengthSpec") {
        updated_content = updated_content.replace(
            "RelationshipPattern,",
            "RelationshipPattern, VariableLengthSpec,"
        );
    }
    
    // Fix all RelationshipPattern constructions by adding variable_length: None
    // Different patterns for different indentation levels
    let patterns = vec![
        // Main parser patterns
        ("properties: None,\n            }", "properties: None,\n                variable_length: None,\n            }"),
        ("properties: None,\n        }", "properties: None,\n            variable_length: None,\n        }"),
        ("properties: None,\n                }", "properties: None,\n                    variable_length: None,\n                }"),
        (".map_or(properties_with_relationship_label, Some),\n        }", ".map_or(properties_with_relationship_label, Some),\n            variable_length: None,\n        }"),
        (".map_or(properties_with_relationship_label, Some),\n                }", ".map_or(properties_with_relationship_label, Some),\n                    variable_length: None,\n                }"),
        // Test patterns  
        ("properties: None,\n                };", "properties: None,\n                    variable_length: None,\n                };"),
        ("properties: None,\n            };", "properties: None,\n                variable_length: None,\n            };"),
        ("properties: None,\n        };", "properties: None,\n            variable_length: None,\n        };"),
        // Other patterns
        ("properties: None,", "properties: None,\n            variable_length: None,"),
    ];
    
    for (from, to) in patterns {
        updated_content = updated_content.replace(from, to);
    }
    
    if updated_content != content {
        fs::write(file_path, updated_content)?;
        println!("Updated: {}", file_path.display());
    }
    
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Walk through all Rust files in brahmand/src
    for entry in WalkDir::new("brahmand/src") {
        let entry = entry?;
        if entry.path().extension().and_then(|s| s.to_str()) == Some("rs") {
            if let Err(e) = fix_relationship_pattern_file(entry.path()) {
                println!("Error processing {}: {}", entry.path().display(), e);
            }
        }
    }
    
    println!("Finished updating RelationshipPattern constructions");
    Ok(())
}
