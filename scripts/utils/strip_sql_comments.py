#!/usr/bin/env python3
"""
⚠️ DEPRECATED: This script is no longer needed as of December 20, 2025

ClickGraph now has NATIVE support for SQL-style comments (both -- and /* */).
Comments are automatically stripped during query processing.

This script remains for historical reference and for processing
query files that need to be used with older versions of ClickGraph.

---

Original Purpose:
Strip SQL-style comments (--) from Cypher query files.

The OpenCypher parser didn't handle SQL-style comments properly,
causing queries to produce empty ASTs. This script was created as
a workaround before native comment support was implemented.

As of v0.5.6+: Use ClickGraph directly - no preprocessing needed!
"""

import argparse
import re
from pathlib import Path


def strip_sql_comments(text):
    """
    Remove SQL-style comments (--) from text while preserving the query.
    
    Handles:
    - Single-line comments (-- comment)
    - Comments at end of lines (code -- comment)
    - Preserves /* */ style comments
    - Preserves strings that contain --
    """
    lines = []
    for line in text.split('\n'):
        # Remove SQL-style comments but keep the line
        # Don't remove if -- is inside a string (basic check)
        if '--' in line:
            # Simple approach: if -- appears outside of quotes, remove everything after it
            in_string = False
            quote_char = None
            cleaned = []
            i = 0
            while i < len(line):
                char = line[i]
                
                # Track string state
                if char in ('"', "'") and (i == 0 or line[i-1] != '\\'):
                    if not in_string:
                        in_string = True
                        quote_char = char
                    elif char == quote_char:
                        in_string = False
                        quote_char = None
                
                # Check for comment start
                if not in_string and i < len(line) - 1 and line[i:i+2] == '--':
                    # Rest of line is a comment
                    break
                
                cleaned.append(char)
                i += 1
            
            line = ''.join(cleaned).rstrip()
        
        # Keep line even if empty (preserves formatting)
        lines.append(line)
    
    return '\n'.join(lines)


def process_file(input_path, output_path=None, in_place=False):
    """Process a single query file."""
    input_path = Path(input_path)
    
    if not input_path.exists():
        print(f"❌ File not found: {input_path}")
        return False
    
    # Read original
    original = input_path.read_text()
    
    # Strip comments
    cleaned = strip_sql_comments(original)
    
    # Determine output path
    if in_place:
        output_path = input_path
    elif output_path is None:
        output_path = input_path.parent / f"{input_path.stem}_cleaned{input_path.suffix}"
    else:
        output_path = Path(output_path)
    
    # Write cleaned version
    output_path.write_text(cleaned)
    
    # Report
    original_lines = len([l for l in original.split('\n') if l.strip().startswith('--')])
    print(f"✅ {input_path.name}")
    print(f"   Removed {original_lines} comment lines")
    print(f"   Output: {output_path}")
    
    return True


def process_directory(directory, pattern="*.cypher", in_place=False):
    """Process all matching files in a directory."""
    directory = Path(directory)
    files = list(directory.glob(pattern))
    
    if not files:
        print(f"⚠️  No files matching '{pattern}' found in {directory}")
        return
    
    print(f"📁 Processing {len(files)} files in {directory}")
    print()
    
    success_count = 0
    for file_path in sorted(files):
        if process_file(file_path, in_place=in_place):
            success_count += 1
        print()
    
    print(f"✅ Successfully processed {success_count}/{len(files)} files")


def main():
    parser = argparse.ArgumentParser(
        description="Strip SQL-style comments from Cypher query files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process single file, create _cleaned version
  python3 strip_sql_comments.py query.cypher
  
  # Process file in-place (overwrite original)
  python3 strip_sql_comments.py query.cypher --in-place
  
  # Process all .cypher files in directory
  python3 strip_sql_comments.py benchmarks/ldbc_snb/queries/IC/ --directory
  
  # Process directory in-place
  python3 strip_sql_comments.py benchmarks/ldbc_snb/queries/IC/ --directory --in-place
        """
    )
    
    parser.add_argument(
        'path',
        help='Input file or directory path'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Output file path (default: <input>_cleaned.cypher)'
    )
    
    parser.add_argument(
        '-d', '--directory',
        action='store_true',
        help='Process all .cypher files in directory'
    )
    
    parser.add_argument(
        '-p', '--pattern',
        default='*.cypher',
        help='File pattern for directory mode (default: *.cypher)'
    )
    
    parser.add_argument(
        '-i', '--in-place',
        action='store_true',
        help='Modify files in place (overwrite originals)'
    )
    
    args = parser.parse_args()
    
    if args.directory:
        process_directory(args.path, pattern=args.pattern, in_place=args.in_place)
    else:
        process_file(args.path, output_path=args.output, in_place=args.in_place)


if __name__ == '__main__':
    main()
