# Script to fix compilation errors from variable_length field additions

# 1. Remove variable_length field from all NodePattern structures
Write-Host "Removing incorrect variable_length fields from NodePattern..." -ForegroundColor Yellow

$filesToFix = @(
    "brahmand\src\open_cypher_parser\mod.rs",
    "brahmand\src\query_planner\logical_expr\mod.rs",
    "brahmand\src\query_planner\logical_plan\match_clause.rs"
)

foreach ($file in $filesToFix) {
    $fullPath = Join-Path $PSScriptRoot $file
    if (Test-Path $fullPath) {
        $content = Get-Content $fullPath -Raw
        
        # Remove variable_length: None, from NodePattern structures (look for NodePattern followed by variable_length within ~200 chars)
        # This is a bit tricky - we'll look for specific patterns
        $content = $content -replace '(?m)(NodePattern\s*\{[^\}]*?properties:\s*[^,\}]*,)\s*variable_length:\s*None,\s*(\})', '$1$2'
        
        Set-Content -Path $fullPath -Value $content -NoNewline
        Write-Host "  Fixed: $file" -ForegroundColor Green
    }
}

# 2. Add variable_length: None to all GraphRel structures
Write-Host "`nAdding missing variable_length fields to GraphRel..." -ForegroundColor Yellow

$graphRelFiles = @(
    "brahmand\src\query_planner\logical_plan\mod.rs",
    "brahmand\src\query_planner\logical_plan\match_clause.rs",
    "brahmand\src\query_planner\analyzer\duplicate_scans_removing.rs",
    "brahmand\src\query_planner\analyzer\graph_join_inference.rs",
    "brahmand\src\query_planner\optimizer\anchor_node_selection.rs"
)

foreach ($file in $graphRelFiles) {
    $fullPath = Join-Path $PSScriptRoot $file
    if (Test-Path $fullPath) {
        $content = Get-Content $fullPath -Raw
        
        # Add variable_length: None, before the closing } of GraphRel structures
        # Look for GraphRel { ... is_rel_anchor: ..., } and add variable_length before }
        $content = $content -replace '(?m)(is_rel_anchor:\s*[^,\}]*,)\s*(\})', "`$1`n            variable_length: None,`n        `$2"
        
        Set-Content -Path $fullPath -Value $content -NoNewline
        Write-Host "  Fixed: $file" -ForegroundColor Green
    }
}

# 3. Fix the missing variable_length in RelationshipPattern in mod.rs line 719
Write-Host "`nFixing missing variable_length in RelationshipPattern..." -ForegroundColor Yellow
$modRsPath = Join-Path $PSScriptRoot "brahmand\src\open_cypher_parser\mod.rs"
if (Test-Path $modRsPath) {
    $content = Get-Content $modRsPath -Raw
    
    # Fix specific RelationshipPattern at line ~719
    $content = $content -replace '(relationship:\s*RelationshipPattern\s*\{[^\}]*?properties:\s*[^,\}]*),\s*(\})', '$1,variable_length: None,$2'
    
    Set-Content -Path $modRsPath -Value $content -NoNewline
    Write-Host "  Fixed RelationshipPattern in mod.rs" -ForegroundColor Green
}

# Fix logical_expr/mod.rs RelationshipPattern
$logicalExprPath = Join-Path $PSScriptRoot "brahmand\src\query_planner\logical_expr\mod.rs"
if (Test-Path $logicalExprPath) {
    $content = Get-Content $logicalExprPath -Raw
    
    # Fix RelationshipPattern construction
    $content = $content -replace '(ast_relationship_pattern\s*=\s*ast::RelationshipPattern\s*\{[^\}]*?properties:\s*[^,\}]*),\s*(\})', '$1,variable_length: None,$2'
    
    Set-Content -Path $logicalExprPath -Value $content -NoNewline
    Write-Host "  Fixed RelationshipPattern in logical_expr/mod.rs" -ForegroundColor Green
}

Write-Host "`nDone! Running cargo check to verify..." -ForegroundColor Cyan
cargo check 2>&1 | Select-String "error" | Select-Object -First 10
