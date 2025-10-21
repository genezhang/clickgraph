# Comprehensive fix script for variable_length compilation errors

Write-Host "===== Fixing Variable Length Compilation Errors =====" -ForegroundColor Cyan

# SECTION 1: Fix GraphRel constructions in various files
Write-Host "`nSection 1: Adding variable_length to GraphRel structures..." -ForegroundColor Yellow

# Fix duplicate_scans_removing.rs
$file = "brahmand\src\query_planner\analyzer\duplicate_scans_removing.rs"
if (Test-Path $file) {
    (Get-Content $file -Raw) -replace '(?ms)(Arc::new\(LogicalPlan::GraphRel\(GraphRel \{[^}]+is_rel_anchor:\s*false,)\s*(\}))', '$1            variable_length: None,$2' | Set-Content $file -NoNewline
    Write-Host "  ✓ Fixed: $file" -ForegroundColor Green
}

# Fix graph_join_inference.rs
$file = "brahmand\src\query_planner\analyzer\graph_join_inference.rs"
if (Test-Path $file) {
    (Get-Content $file -Raw) -replace '(?ms)(Arc::new\(LogicalPlan::GraphRel\(GraphRel \{[^}]+is_rel_anchor:\s*\w+,)\s*(\}))', '$1            variable_length: None,$2' | Set-Content $file -NoNewline
    Write-Host "  ✓ Fixed: $file" -ForegroundColor Green
}

# Fix anchor_node_selection.rs - this has 5 instances
$file = "brahmand\src\query_planner\optimizer\anchor_node_selection.rs"
if (Test-Path $file) {
    $content = Get-Content $file -Raw
    # Replace all occurrences
    $content = $content -replace '(?ms)(GraphRel \{[^}]*is_rel_anchor:\s*\w+,)\s*(\})', '$1                variable_length: None,$2'
    Set-Content $file -Value $content -NoNewline
    Write-Host "  ✓ Fixed: $file" -ForegroundColor Green
}

# Fix logical_plan/mod.rs - has 2 instances
$file = "brahmand\src\query_planner\logical_plan\mod.rs"
if (Test-Path $file) {
    $content = Get-Content $file -Raw
    $content = $content -replace '(?ms)(GraphRel \{[^}]*is_rel_anchor:\s*\w+,)\s*(\})', '$1            variable_length: None,$2'
    Set-Content $file -Value $content -NoNewline
    Write-Host "  ✓ Fixed: $file" -ForegroundColor Green
}

# SECTION 2: Fix RelationshipPattern constructions
Write-Host "`nSection 2: Adding variable_length to RelationshipPattern structures..." -ForegroundColor Yellow

# Fix logical_expr/mod.rs - has RelationshipPattern constructions
$file = "brahmand\src\query_planner\logical_expr\mod.rs"
if (Test-Path $file) {
    $content = Get-Content $file -Raw
    $content = $content -replace '(?ms)(ast::RelationshipPattern \{[^}]*properties:\s*[^,}]+),\s*(\})', '$1,            variable_length: None,$2'
    Set-Content $file -Value $content -NoNewline
    Write-Host "  ✓ Fixed: $file" -ForegroundColor Green
}

# Fix open_cypher_parser/mod.rs - has RelationshipPattern construction
$file = "brahmand\src\open_cypher_parser\mod.rs"
if (Test-Path $file) {
    $content = Get-Content $file -Raw
    # Fix the specific RelationshipPattern
    $content = $content -replace '(relationship:\s*RelationshipPattern\s*\{\s*name:\s*[^,]+,\s*direction:\s*[^,]+,\s*label:\s*[^,]+,\s*properties:\s*[^,}]+),\s*(\})', '$1,                        variable_length: None,$2'
    Set-Content $file -Value $content -NoNewline
    Write-Host "  ✓ Fixed: $file" -ForegroundColor Green
}

Write-Host "`nSection 3: Verifying changes..." -ForegroundColor Yellow
Write-Host "Running cargo check..." -ForegroundColor Cyan

# Run cargo check and show only error count
$errors = cargo check 2>&1 | Out-String
$errorCount = ($errors | Select-String "error" | Measure-Object).Count
Write-Host "Errors remaining: $errorCount" -ForegroundColor $(if ($errorCount -eq 0) {"Green"} else {"Yellow"})

if ($errorCount -gt 0) {
    Write-Host "`nShowing first few errors:" -ForegroundColor Yellow
    cargo check 2>&1 | Select-String "error" | Select-Object -First 5 | ForEach-Object { Write-Host $_.Line -ForegroundColor Red }
}
