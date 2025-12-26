"""
Meta-tests to ensure all schema types are adequately tested.

This test file validates that critical features are tested across
ALL supported schema patterns (traditional, denormalized, FK-edge, etc.).

Purpose: Prevent regressions like the Dec 22, 2025 denormalized VLP breakage,
where changes optimized for traditional schemas broke denormalized schemas.
"""

import os
import re
import pytest
from pathlib import Path


def find_test_files():
    """Find all integration test files."""
    test_dir = Path(__file__).parent.parent / "integration"
    return list(test_dir.glob("test_*.py"))


def extract_test_functions(file_path):
    """Extract test function names and their decorators from a Python file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find all test functions with their decorators
    pattern = r'(@pytest\.mark\.\w+[^\n]*\n\s*)*def (test_\w+)\('
    matches = re.findall(pattern, content)
    
    tests = []
    for match in matches:
        decorator = match[0].strip() if match[0] else ""
        test_name = match[1]
        tests.append((test_name, decorator))
    
    return tests


class TestSchemaCoverage:
    """Validate that critical features are tested across all schema types."""
    
    def test_vlp_has_denormalized_tests(self):
        """Ensure variable-length paths are tested with denormalized schemas."""
        denorm_file = Path(__file__).parent.parent / "integration" / "test_denormalized_edges.py"
        
        if not denorm_file.exists():
            pytest.fail(f"Missing denormalized test file: {denorm_file}")
        
        with open(denorm_file, 'r') as f:
            content = f.read()
        
        # Check for VLP test class
        assert "class TestDenormalizedVariableLengthPaths" in content, \
            "Missing TestDenormalizedVariableLengthPaths class in denormalized tests"
        
        # Check for actual VLP tests (not just xfail placeholders)
        vlp_tests = [
            "test_variable_path_with_denormalized_properties",
            "test_variable_path_cte_uses_denormalized_props",
        ]
        
        for test_name in vlp_tests:
            assert f"def {test_name}" in content, \
                f"Missing VLP test: {test_name}"
    
    def test_no_xfail_on_critical_features(self):
        """Ensure critical features are not marked as xfail."""
        critical_patterns = [
            # VLP is a core feature - should never be xfailed
            (r'@pytest\.mark\.xfail.*\n\s*def test_variable_path', 
             "Variable-length paths should not be xfailed"),
            
            # OPTIONAL MATCH is an advertised feature
            (r'@pytest\.mark\.xfail.*\n\s*def test_optional_match',
             "OPTIONAL MATCH should not be xfailed"),
        ]
        
        test_files = find_test_files()
        violations = []
        
        for test_file in test_files:
            with open(test_file, 'r') as f:
                content = f.read()
            
            for pattern, message in critical_patterns:
                matches = re.findall(pattern, content)
                if matches:
                    violations.append(f"{test_file.name}: {message}")
        
        if violations:
            pytest.fail(
                "Found xfail markers on critical features:\n" + 
                "\n".join(f"  - {v}" for v in violations) +
                "\n\nCritical features must be working. Either fix the bug or revert the breaking change."
            )
    
    def test_vlp_tests_exist_for_both_schemas(self):
        """Ensure VLP tests exist for both traditional and denormalized schemas."""
        test_dir = Path(__file__).parent.parent / "integration"
        
        # Traditional VLP tests
        traditional_file = test_dir / "test_variable_paths.py"
        assert traditional_file.exists(), \
            "Missing test_variable_paths.py for traditional schema VLP tests"
        
        # Denormalized VLP tests  
        denorm_file = test_dir / "test_denormalized_edges.py"
        assert denorm_file.exists(), \
            "Missing test_denormalized_edges.py for denormalized schema VLP tests"
        
        # Check both files have VLP test functions
        with open(traditional_file, 'r') as f:
            trad_content = f.read()
        assert "test_variable" in trad_content.lower() or "test_vlp" in trad_content.lower(), \
            "No VLP tests found in test_variable_paths.py"
        
        with open(denorm_file, 'r') as f:
            denorm_content = f.read()
        assert "test_variable_path" in denorm_content, \
            "No VLP tests found in test_denormalized_edges.py"


class TestCodeComments:
    """Validate that critical code sections have proper documentation."""
    
    def test_cte_extraction_has_schema_warning(self):
        """Ensure cte_extraction.rs has clear warnings about multi-schema support."""
        cte_file = Path(__file__).parent.parent.parent / "src" / "render_plan" / "cte_extraction.rs"
        
        if not cte_file.exists():
            pytest.skip("Source file not found in test environment")
        
        with open(cte_file, 'r') as f:
            content = f.read()
        
        # Check for critical warning comments
        required_markers = [
            "CRITICAL",  # Flags critical section
            "TRADITIONAL SCHEMA",  # Explains traditional pattern
            "DENORMALIZED SCHEMA",  # Explains denormalized pattern
            "is_denormalized",  # Must check this flag
        ]
        
        # Find the node ID selection section
        if "let start_id_col = if" in content:
            section_start = content.find("let start_id_col = if")
            section = content[max(0, section_start - 2000):section_start + 500]
            
            missing = [marker for marker in required_markers if marker not in section]
            
            if missing:
                pytest.fail(
                    f"cte_extraction.rs node ID selection lacks documentation.\n"
                    f"Missing markers: {', '.join(missing)}\n"
                    f"This section has broken multiple times - MUST document both schema types!"
                )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
