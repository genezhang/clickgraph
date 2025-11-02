#!/usr/bin/env python3
"""
Test runner convenience script - Run from project root.

This script delegates to the actual test runner in tests/python/
with the correct working directory.

Usage:
    python run_tests.py              # Run all tests
    python run_tests.py <pattern>    # Run tests matching pattern
"""

import sys
import os
from pathlib import Path

# Change to tests/python directory
tests_dir = Path(__file__).parent / "tests" / "python"
os.chdir(tests_dir)

# Import and run the actual test runner
sys.path.insert(0, str(tests_dir))
from run_all_tests import main

if __name__ == "__main__":
    main()
