#!/usr/bin/env python3
"""
Remove emoji characters from test files safely
"""
import os
from pathlib import Path
import re

# Emoji replacements - specific characters
replacements = {
    '✅': '[OK]',
    '❌': '[FAIL]',
    '🔍': '[INFO]',
    '🧪': '[TEST]',
    '🎯': '[TARGET]',
    '✓': '[OK]',
    '✗': '[FAIL]',
    '⚠️': '[WARN]',
    '⚠': '[WARN]',
    '🎉': '[SUCCESS]',
    '💥': '[ERROR]',
    '🚀': '[START]',
    '🛑': '[STOP]',
    '⏳': '[WAIT]',
    '1️⃣': '1.',
    '2️⃣': '2.',
    '3️⃣': '3.',
    '4️⃣': '4.',
    '5️⃣': '5.',
    '\u2713': '[OK]',
    '\u274c': '[FAIL]',
    '\u2717': '[FAIL]',
    '\u26a0': '[WARN]',
}

def fix_file(filepath):
    """Remove emojis from a single file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # First, apply specific replacements
        for emoji, replacement in replacements.items():
            content = content.replace(emoji, replacement)
        
        # Then remove any remaining variation selectors and combining characters
        # \uFE0F is a variation selector, \u20E3 is combining enclosing keycap
        content = re.sub(r'[\uFE0F\u20E3]', '', content)
        
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
