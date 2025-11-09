#!/usr/bin/env pwsh
# Script to add `is_optional: None,` to all GraphRel initializations

$files = @(
    "brahmand\src\query_planner\optimizer\filter_into_graph_rel.rs",
    "brahmand\src\query_planner\optimizer\anchor_node_selection.rs",
    "brahmand\src\query_planner\analyzer\graph_traversal_planning.rs",
    "brahmand\src\query_planner\analyzer\graph_join_inference.rs"
)

foreach ($file in $files) {
    $content = Get-Content $file -Raw
    
    # Replace pattern: `labels: ...` followed by newline and closing brace
    # Add `is_optional: None,` before the closing brace
    $pattern = '(\s+labels:\s+[^,\n]+,)\s*\n(\s+)\}'
    $replacement = '$1' + "`n" + '$2is_optional: None,' + "`n" + '$2}'
    
    $newContent = $content -replace $pattern, $replacement
    
    if ($newContent -ne $content) {
        Set-Content -Path $file -Value $newContent -NoNewline
        Write-Host "✅ Updated $file"
    } else {
        Write-Host "⚠️  No changes needed in $file"
    }
}

Write-Host "`nDone! Run cargo build to verify."
