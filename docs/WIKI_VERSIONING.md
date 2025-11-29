# Wiki Documentation Versioning

This document describes how we version and archive wiki documentation for ClickGraph releases.

## Philosophy

- **Main wiki** (`docs/wiki/`) - Always reflects latest development, may include unreleased features
- **Archived wikis** (`docs/wiki-versions/vX.Y.Z/`) - Frozen snapshots at release time

This allows users to:
- Access documentation matching their installed version
- Preview upcoming features in development docs
- Maintain compatibility with specific Docker image tags

## Workflow

### 1. During Development

Work normally in `docs/wiki/`. Mark unreleased features with version tags:

```markdown
- **[New Feature](Feature-Guide.md)** - Cool new capability (v0.5.2+)
```

### 2. When Releasing (e.g., v0.5.2)

Run the archive script:

```bash
./scripts/release/archive_docs.sh 0.5.2
```

This will:
1. Create `docs/wiki-versions/v0.5.2/wiki/`
2. Copy all current wiki files
3. Add version banners to each file
4. Stage changes for git

### 3. Commit and Tag

```bash
# Review changes
git diff --staged

# Commit archive
git commit -m "Archive wiki for v0.5.2 release"

# Update version in Cargo.toml (if not already done)
# Then tag the release
git tag -a v0.5.2 -m "Release v0.5.2"

# Push everything
git push origin main --tags
```

### 4. Update README (Optional)

Add version reference to README.md:

```markdown
## Documentation

- **[Latest Documentation](docs/wiki/Home.md)** - Development version
- **Version-Specific Docs**:
  - [v0.5.2](docs/wiki-versions/v0.5.2/wiki/Home.md) | [Docker](https://hub.docker.com/r/genezhang/clickgraph/tags?name=0.5.2)
  - [v0.5.1](docs/wiki-versions/v0.5.1/wiki/Home.md) | [Docker](https://hub.docker.com/r/genezhang/clickgraph/tags?name=0.5.1)
```

## Directory Structure

```
docs/
├── wiki/                    # Main wiki (latest development)
│   ├── Home.md
│   ├── Schema-Basics.md
│   └── ...
└── wiki-versions/           # Historical versions
    ├── v0.5.0/
    │   └── wiki/
    ├── v0.5.1/
    │   └── wiki/
    └── v0.5.2/
        └── wiki/
```

## Script Usage

```bash
# Archive current wiki for v0.5.2 release
./scripts/release/archive_docs.sh 0.5.2

# Or with 'v' prefix (automatically stripped)
./scripts/release/archive_docs.sh v0.5.2
```

**Script validates:**
- Version format (X.Y.Z)
- Warns if archive already exists
- Adds version banners automatically
- Shows next steps

## Version Banners

Each archived file gets a banner at the top:

```markdown
> **Note**: This documentation is for ClickGraph v0.5.2. [View latest docs →](../../wiki/Home.md)
```

This helps users understand they're viewing historical documentation.

## Best Practices

1. **Archive on every release** - Even minor releases (0.5.1 → 0.5.2)
2. **Archive before tagging** - Ensures git tag matches archived docs
3. **Update CHANGELOG** - Document what features are in each release
4. **Test archived links** - Verify internal links still work in archived version
5. **Keep main wiki current** - Don't freeze development docs

## Retroactive Archiving

If you need to create an archive for a past release:

```bash
# Checkout docs from release tag
git checkout v0.5.1 -- docs/wiki

# Create archive
mkdir -p docs/wiki-versions/v0.5.1
cp -r docs/wiki docs/wiki-versions/v0.5.1/

# Restore current docs
git checkout main -- docs/wiki

# Manually add banners and commit
```

## FAQ

**Q: Should we archive docs for patch releases (0.5.2 → 0.5.3)?**
A: Yes, if there are documentation changes. Skip if only code changes.

**Q: What if docs have bugs in an archived version?**
A: Generally leave them as-is (historical accuracy). For critical errors, you can update the archived version and note the correction.

**Q: Can we delete old archives?**
A: Keep at least 2-3 recent major versions. Older versions can be removed or moved to git history only.

**Q: What about docs for unreleased features?**
A: Keep them in main wiki with version tags (e.g., "v0.5.3+"). Archive captures them at release time.

## Integration with Docker Hub

Reference archived docs in Docker Hub image descriptions:

```
Docker tag: v0.5.2
Documentation: https://github.com/genezhang/clickgraph/tree/main/docs/wiki-versions/v0.5.2/wiki
Release Notes: https://github.com/genezhang/clickgraph/blob/main/CHANGELOG.md#052---2025-11-29
```

## Maintenance

Review archived docs annually:
- Remove very old versions (>2 years)
- Verify links still work
- Update banner styling if needed
