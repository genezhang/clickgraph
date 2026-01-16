#!/usr/bin/env python3
"""
Dependency Analysis for plan_builder.rs Refactoring

This script analyzes the dependencies between functions in plan_builder.rs
to inform the safe extraction order during refactoring.
"""

import re
from pathlib import Path
from typing import Dict, Set, List

class DependencyAnalyzer:
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.functions: Dict[str, Dict] = {}
        self.dependencies: Dict[str, Set[str]] = {}

    def analyze(self):
        """Analyze the file for function definitions and calls"""
        content = self.file_path.read_text()

        # Find all function definitions
        func_pattern = r'fn\s+(\w+)\s*\('
        functions = re.findall(func_pattern, content)

        print(f"Found {len(functions)} functions in {self.file_path}")

        # For each function, find what it calls
        for func in functions:
            self.functions[func] = {
                'defined': True,
                'calls': set()
            }

            # Find function calls within this function's scope
            func_start = content.find(f'fn {func}(')
            if func_start == -1:
                continue

            # Find the next function definition to determine scope
            next_func = content.find('fn ', func_start + 1)
            if next_func == -1:
                next_func = len(content)

            func_body = content[func_start:next_func]

            # Find all function calls in this body
            call_pattern = r'\b(\w+)\s*\('
            calls = re.findall(call_pattern, func_body)

            # Filter out language constructs and keep only our functions
            our_functions = set(functions)
            actual_calls = set()
            for call in calls:
                if call in our_functions and call != func:  # Avoid self-reference
                    actual_calls.add(call)

            self.functions[func]['calls'] = actual_calls
            self.dependencies[func] = actual_calls

    def get_pure_utilities(self) -> List[str]:
        """Identify functions that don't depend on LogicalPlan"""
        pure_funcs = []
        for func, info in self.functions.items():
            calls = info['calls']
            # A function is "pure utility" if it doesn't call other functions
            # or only calls other pure utilities
            if not calls:
                pure_funcs.append(func)
        return pure_funcs

    def print_analysis(self):
        """Print dependency analysis"""
        print("\n=== DEPENDENCY ANALYSIS ===")
        print(f"Total functions: {len(self.functions)}")

        pure_utils = self.get_pure_utilities()
        print(f"Pure utilities (no dependencies): {len(pure_utils)}")
        for func in sorted(pure_utils):
            print(f"  - {func}")

        print("\nFunction dependencies:")
        for func, deps in sorted(self.dependencies.items()):
            if deps:
                print(f"  {func} -> {sorted(deps)}")
            else:
                print(f"  {func} -> (none)")

    def get_extraction_order(self) -> List[str]:
        """Suggest extraction order based on dependencies"""
        # Simple topological sort - functions with no dependencies first
        order = []
        remaining = set(self.functions.keys())

        while remaining:
            # Find functions with no remaining dependencies
            available = []
            for func in remaining:
                deps = self.dependencies[func]
                if not deps.intersection(remaining):
                    available.append(func)

            if not available:
                # Circular dependency detected
                print("Warning: Circular dependencies detected in remaining functions:")
                for func in sorted(remaining):
                    deps = self.dependencies[func].intersection(remaining)
                    if deps:
                        print(f"  {func} -> {sorted(deps)}")
                break

            # Sort for consistent ordering
            available.sort()
            order.extend(available)
            remaining -= set(available)

        return order

def main():
    analyzer = DependencyAnalyzer("/home/gz/clickgraph/src/render_plan/plan_builder.rs")
    analyzer.analyze()
    analyzer.print_analysis()

    order = analyzer.get_extraction_order()
    print("\n=== EXTRACTION ORDER SUGGESTION ===")
    print("Suggested order (dependencies first):")
    for i, func in enumerate(order, 1):
        print("2d")

if __name__ == "__main__":
    main()