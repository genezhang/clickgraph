#!/usr/bin/env python3
"""
Remove emoji characters from test files safely
"""
import os
from pathlib import Path

# Emoji replacements
replacements = {
    '✅': '[OK]',
    '❌': '[FAIL]',
    '🔍': '[INFO]',
    '🧪': '[TEST]',
    '🎯': '[TARGET]',
    '\u2713': '[OK]',
    '\u274c': '[FAIL]',
}

def fix_file(filepath):
    """Remove emojis from a single file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        for emoji, replacement in replacements.items():
            content = content.replace(emoji, replacement)
        
        if content != original_content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        return False
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return False

def main():
    test_dir = Path('tests/python')
    fixed_count = 0
    
    for test_file in test_dir.glob('*.py'):
        if fix_file(test_file):
            print(f"Fixed: {test_file.name}")
            fixed_count += 1
    
    print(f"\nFixed {fixed_count} files")

if __name__ == '__main__':
    main()
