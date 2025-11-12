import sys

# Read the file
with open('e2e_framework.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace all emoji
replacements = {
    '📦': '[SETUP]',
    '✓': '[OK]',
    '✗': '[ERROR]',
    '✅': '[COMPLETE]',
    '🔧': '[DEBUG]',
    '🧹': '[CLEANUP]',
    '⚠': '[WARNING]'
}

for emoji, text in replacements.items():
    content = content.replace(emoji, text)

# Write back
with open('e2e_framework.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Emojis removed successfully")
