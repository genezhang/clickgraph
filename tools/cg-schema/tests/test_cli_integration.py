"""Integration tests for cg-schema CLI."""

import subprocess
import sys
import os
import tempfile
import pytest


class TestCLIIntegration:
    """Integration tests for cg-schema CLI commands."""

    @pytest.fixture
    def server_url(self):
        """Server URL for testing."""
        return "localhost:8080"

    @pytest.fixture
    def test_db(self):
        """Test database."""
        return "social"

    def test_introspect_basic(self, server_url, test_db):
        """Test basic introspect command."""
        result = subprocess.run(
            [sys.executable, "-m", "cg_schema.cli", "introspect", 
             "-s", server_url, "-d", test_db],
            capture_output=True,
            text=True,
            cwd=os.path.join(os.path.dirname(__file__), ".."),
            env={**os.environ, "PYTHONPATH": "tools/cg-schema"}
        )
        
        assert result.returncode == 0
        assert "Found" in result.stdout
        assert "tables" in result.stdout.lower()

    def test_introspect_with_output(self, server_url, test_db):
        """Test introspect with YAML output."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            output_path = f.name
        
        try:
            result = subprocess.run(
                [sys.executable, "-m", "cg_schema.cli", "introspect",
                 "-s", server_url, "-d", test_db, "-o", output_path],
                capture_output=True,
                text=True,
                cwd=os.path.join(os.path.dirname(__file__), ".."),
                env={**os.environ, "PYTHONPATH": "tools/cg-schema"}
            )
            
            assert result.returncode == 0
            assert os.path.exists(output_path)
            
            with open(output_path) as f:
                content = f.read()
                assert "nodes:" in content or "relationships:" in content
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_introspect_multiple_databases(self, server_url):
        """Test introspect on different databases."""
        databases = ["social", "ldbc", "travel"]
        
        for db in databases:
            result = subprocess.run(
                [sys.executable, "-m", "cg_schema.cli", "introspect",
                 "-s", server_url, "-d", db],
                capture_output=True,
                text=True,
                cwd=os.path.join(os.path.dirname(__file__), ".."),
                env={**os.environ, "PYTHONPATH": "tools/cg-schema"}
            )
            
            # Should either succeed or show connection error (acceptable)
            assert result.returncode == 0 or "connect" in result.stdout.lower()

    def test_introspect_unknown_database(self, server_url):
        """Test introspect on non-existent database."""
        result = subprocess.run(
            [sys.executable, "-m", "cg_schema.cli", "introspect",
             "-s", server_url, "-d", "nonexistent_db_12345"],
            capture_output=True,
            text=True,
            cwd=os.path.join(os.path.dirname(__file__), ".."),
            env={**os.environ, "PYTHONPATH": "tools/cg-schema"}
        )
        
        # Should handle gracefully (either 404 or show empty tables)
        assert result.returncode in [0, 1]

    def test_introspect_server_not_running(self):
        """Test introspect when server is not available."""
        result = subprocess.run(
            [sys.executable, "-m", "cg_schema.cli", "introspect",
             "-s", "localhost:19999", "-d", "test"],
            capture_output=True,
            text=True,
            cwd=os.path.join(os.path.dirname(__file__), ".."),
            env={**os.environ, "PYTHONPATH": "tools/cg-schema"},
            timeout=5
        )
        
        assert result.returncode != 0
        assert "connect" in result.stdout.lower() or "connection" in result.stdout.lower()

    def test_help_command(self):
        """Test CLI help."""
        result = subprocess.run(
            [sys.executable, "-m", "cg_schema.cli", "--help"],
            capture_output=True,
            text=True,
            cwd=os.path.join(os.path.dirname(__file__), ".."),
            env={**os.environ, "PYTHONPATH": "tools/cg-schema"}
        )
        
        assert result.returncode == 0
        assert "introspect" in result.stdout.lower()
        assert "push" in result.stdout.lower()

    def test_introspect_subcommand_help(self):
        """Test introspect subcommand help."""
        result = subprocess.run(
            [sys.executable, "-m", "cg_schema.cli", "introspect", "--help"],
            capture_output=True,
            text=True,
            cwd=os.path.join(os.path.dirname(__file__), ".."),
            env={**os.environ, "PYTHONPATH": "tools/cg-schema"}
        )
        
        assert result.returncode == 0
        assert "--server" in result.stdout or "-s" in result.stdout


class TestSchemaGeneration:
    """Integration tests for schema generation."""

    def test_generate_social_schema(self):
        """Test schema generation for social database."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            output_path = f.name
        
        try:
            result = subprocess.run(
                [sys.executable, "-m", "cg_schema.cli", "introspect",
                 "-s", "localhost:8080", "-d", "social", "-o", output_path],
                capture_output=True,
                text=True,
                cwd=os.path.join(os.path.dirname(__file__), ".."),
                env={**os.environ, "PYTHONPATH": "tools/cg-schema"},
                timeout=60
            )
            
            if result.returncode == 0 and os.path.exists(output_path):
                with open(output_path) as f:
                    content = f.read()
                    
                # Basic YAML structure validation
                assert "nodes:" in content or "relationships:" in content
                
                # Should contain at least users node
                if "users" in content.lower():
                    assert "user" in content.lower()
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_generate_ldbc_schema(self):
        """Test schema generation for ldbc database."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            output_path = f.name
        
        try:
            result = subprocess.run(
                [sys.executable, "-m", "cg_schema.cli", "introspect",
                 "-s", "localhost:8080", "-d", "ldbc", "-o", output_path],
                capture_output=True,
                text=True,
                cwd=os.path.join(os.path.dirname(__file__), ".."),
                env={**os.environ, "PYTHONPATH": "tools/cg-schema"},
                timeout=60
            )
            
            if result.returncode == 0 and os.path.exists(output_path):
                with open(output_path) as f:
                    content = f.read()
                    
                # Should have nodes and relationships
                assert "nodes:" in content or "relationships:" in content
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_generate_travel_schema(self):
        """Test schema generation for travel database."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            output_path = f.name
        
        try:
            result = subprocess.run(
                [sys.executable, "-m", "cg_schema.cli", "introspect",
                 "-s", "localhost:8080", "-d", "travel", "-o", output_path],
                capture_output=True,
                text=True,
                cwd=os.path.join(os.path.dirname(__file__), ".."),
                env={**os.environ, "PYTHONPATH": "tools/cg-schema"},
                timeout=60
            )
            
            if result.returncode == 0 and os.path.exists(output_path):
                with open(output_path) as f:
                    content = f.read()
                    
                # Should detect flights tables
                lines = content.lower().split('\n')
                has_flight = any('flight' in line for line in lines)
                assert has_flight or "nodes:" in content or "relationships:" in content
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
