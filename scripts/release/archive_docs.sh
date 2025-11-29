#!/bin/bash
# Archive wiki documentation for a specific release version
# Usage: ./scripts/release/archive_docs.sh <version>
# Example: ./scripts/release/archive_docs.sh 0.5.2

set -e

VERSION=$1

if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  echo "Example: $0 0.5.2"
  exit 1
fi

# Remove 'v' prefix if present
VERSION=${VERSION#v}

# Validate version format (X.Y.Z)
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Error: Invalid version format. Expected X.Y.Z (e.g., 0.5.2)"
  exit 1
fi

ARCHIVE_DIR="docs/wiki-versions/v$VERSION"
WIKI_SOURCE="docs/wiki"

echo "📦 Archiving wiki documentation for v$VERSION..."

# Check if archive already exists
if [ -d "$ARCHIVE_DIR" ]; then
  read -p "Archive for v$VERSION already exists. Overwrite? (y/N) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
  fi
  rm -rf "$ARCHIVE_DIR"
fi

# Create archive directory
mkdir -p "$ARCHIVE_DIR"

# Copy current wiki
echo "📋 Copying wiki files..."
cp -r "$WIKI_SOURCE" "$ARCHIVE_DIR/"

# Add version banner to all markdown files
echo "🏷️  Adding version banners..."
BANNER="> **Note**: This documentation is for ClickGraph v$VERSION. [View latest docs →](../../wiki/Home.md)"

find "$ARCHIVE_DIR/wiki" -name "*.md" -type f | while read -r file; do
  # Insert banner at the top of each file
  echo "$BANNER" | cat - "$file" > temp && mv temp "$file"
  echo "  ✓ $(basename "$file")"
done

# Count archived files
FILE_COUNT=$(find "$ARCHIVE_DIR/wiki" -name "*.md" -type f | wc -l)
echo "✅ Archived $FILE_COUNT wiki files to $ARCHIVE_DIR/"

# Stage changes
echo "📝 Staging changes..."
git add "$ARCHIVE_DIR"

# Show status
echo ""
echo "📊 Archive Status:"
echo "  Version: v$VERSION"
echo "  Location: $ARCHIVE_DIR/"
echo "  Files: $FILE_COUNT markdown files"
echo ""
echo "Next steps:"
echo "  1. Review changes: git diff --staged"
echo "  2. Commit: git commit -m 'Archive wiki for v$VERSION release'"
echo "  3. Tag release: git tag -a v$VERSION -m 'Release v$VERSION'"
echo "  4. Push: git push origin main --tags"
echo ""
echo "✨ Done!"
