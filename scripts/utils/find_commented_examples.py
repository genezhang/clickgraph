"""
Find and report all commented-out examples in wiki documentation.

This script helps maintain visibility of features that need to be implemented
before documentation examples can be uncommented.
"""

import re
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple


def find_commented_examples(wiki_dir: Path) -> Dict[str, List[Tuple[int, str, str]]]:
    """
    Find all commented examples in markdown files.
    
    Returns:
        Dict mapping filenames to list of (line_number, category, description) tuples
    """
    results = defaultdict(list)
    
    # Pattern to match our comment format
    warning_pattern = re.compile(r'<!--\s*⚠️\s*([^-]+)\s*-\s*([^\n]+)')
    
    for md_file in wiki_dir.glob("*.md"):
        with open(md_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        in_comment = False
        comment_start_line = 0
        category = ""
        description = ""
        
        for i, line in enumerate(lines, 1):
            # Check for start of our special comments
            match = warning_pattern.search(line)
            if match:
                in_comment = True
                comment_start_line = i
                category = match.group(1).strip()
                description = match.group(2).strip()
            
            # Check for end of comment
            if in_comment and '-->' in line:
                results[md_file.name].append((comment_start_line, category, description))
                in_comment = False
    
    return results


def generate_report(results: Dict[str, List[Tuple[int, str, str]]]) -> str:
    """Generate markdown report of commented examples."""
    
    if not results:
        return "✅ No commented-out examples found!"
    
    # Count by category
    category_counts = defaultdict(int)
    for examples in results.values():
        for _, category, _ in examples:
            category_counts[category] += 1
    
    # Build report
    report = ["# Commented Documentation Examples Report\n"]
    report.append(f"**Total Files**: {len(results)}\n")
    report.append(f"**Total Examples**: {sum(len(v) for v in results.values())}\n")
    report.append("\n## Summary by Category\n")
    
    for category, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        report.append(f"- **{category}**: {count} examples\n")
    
    report.append("\n## Details by File\n")
    
    for filename in sorted(results.keys()):
        examples = results[filename]
        report.append(f"\n### {filename}\n")
        report.append(f"**Count**: {len(examples)} commented examples\n\n")
        
        for line_num, category, description in sorted(examples):
            report.append(f"- **Line {line_num}**: [{category}] {description}\n")
    
    report.append("\n---\n")
    report.append("\n**Next Steps**:\n")
    report.append("1. Review each commented example\n")
    report.append("2. Create GitHub issues for FUTURE FEATURE items\n")
    report.append("3. Fix or replace NON-EXISTENT PROPERTY examples\n")
    report.append("4. Test and uncomment when features are ready\n")
    report.append("\nSee `docs/development/maintaining-commented-examples.md` for details.\n")
    
    return "".join(report)


def main():
    """Main entry point."""
    wiki_dir = Path("docs/wiki")
    
    if not wiki_dir.exists():
        print(f"Error: Wiki directory not found at {wiki_dir}")
        return
    
    print("Scanning wiki documentation for commented examples...\n")
    
    results = find_commented_examples(wiki_dir)
    report = generate_report(results)
    
    # Print to console
    print(report)
    
    # Save to file
    output_file = Path("docs/COMMENTED_EXAMPLES_REPORT.md")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n✅ Report saved to: {output_file}")


if __name__ == "__main__":
    main()
