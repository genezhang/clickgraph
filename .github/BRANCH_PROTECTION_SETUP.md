# Branch Protection Setup Instructions

**Updated**: January 12, 2026

## Why Branch Protection is Needed

As of January 2026, we've established a **mandatory branch-based workflow** requiring:
- All development on feature branches
- Pull requests for all changes
- Human code review before merge
- No direct commits to main

To enforce this, GitHub branch protection rules must be configured via the UI.

---

## Required Configuration Steps

### 1. Enable Branch Protection for `main`

Go to: **Repository Settings → Branches → Branch protection rules → Add rule**

**Branch name pattern**: `main`

### 2. Enable These Settings:

#### Protect matching branches
- ✅ **Require a pull request before merging**
  - ✅ **Require approvals**: 1 (minimum)
  - ✅ **Dismiss stale pull request approvals when new commits are pushed**
  - ✅ **Require review from Code Owners** (optional, if CODEOWNERS file exists)
  - ⚠️ **Do NOT** allow bypassing via "Allow specified actors to bypass"

#### Require status checks before merging
- ✅ **Require status checks to pass before merging**
- ✅ **Require branches to be up to date before merging**
- **Required status checks**:
  - `build` (from ci.yml)
  - `test` (from ci.yml) 
  - (Add other CI jobs as they become required)

#### Additional Settings
- ✅ **Require conversation resolution before merging**
- ✅ **Require signed commits** (optional, for security)
- ✅ **Require linear history** (optional, enforces rebase/squash)
- ✅ **Include administrators** (no one can bypass, including admins)

#### Restrictions
- ✅ **Do not allow bypassing the above settings**
- ✅ **Restrict who can push to matching branches** (optional, for extra security)

#### Rules applied to everyone
- ✅ **Restrict deletions** (prevent accidental branch deletion)
- ✅ **Restrict creations** (only create branches via proper workflow)
- ✅ **Restrict updates** (no force pushes)
- ⚠️ **Block force pushes** - CRITICAL!

---

## Current Workflow Configuration

### Auto-Approve Workflow (Modified)
**File**: `.github/workflows/auto-approve.yml`

**Status**: Auto-merge DISABLED (as of Jan 12, 2026)

**Behavior**:
- Bot will auto-approve docs-only PRs (markdown, YAML, config files)
- Bot approval counts toward "1 required approval"
- **But**: Human review is still recommended before manual merge
- **Auto-merge job is commented out** - requires manual merge via GitHub UI

**Rationale**: 
Even documentation changes should be reviewed by a human to ensure:
- Technical accuracy
- Completeness
- No accidental exposure of sensitive information
- Consistency with project standards

### To Re-Enable Auto-Merge (NOT RECOMMENDED)
If you want to re-enable auto-merge for docs-only PRs:

1. Edit `.github/workflows/auto-approve.yml`
2. Uncomment the `auto-merge` job (lines with `#`)
3. Commit the change
4. This will only work if:
   - Branch protection allows it
   - Required CI checks pass
   - PR has required approvals (can be bot)

---

## Verification Steps

After configuring branch protection:

### Test 1: Try Direct Commit to Main (Should FAIL)
```bash
git checkout main
echo "test" >> test.txt
git add test.txt
git commit -m "test: direct commit"
git push origin main  # Should be rejected
```

**Expected**: Push rejected with message about branch protection

### Test 2: PR Without Approval (Should BLOCK merge)
1. Create feature branch
2. Make changes and push
3. Create PR
4. Try to merge without approval
5. **Expected**: Merge button disabled or blocked

### Test 3: PR With Approval (Should ALLOW merge)
1. Create feature branch with changes
2. Create PR
3. Get approval from reviewer (or bot for docs-only)
4. **Expected**: Can merge manually

---

## Emergency Override (Use Sparingly!)

If you absolutely must bypass branch protection:

### Option 1: Temporary Admin Override
1. Temporarily disable "Include administrators" in branch protection
2. Make emergency commit
3. **Immediately** re-enable "Include administrators"

### Option 2: Emergency Hotfix PR
```bash
git checkout main
git pull
git checkout -b hotfix/critical-issue
# Fix issue
git push origin hotfix/critical-issue
# Create PR with "HOTFIX" label
# Fast-track review and approval
# Merge immediately
```

**Document**: Log all emergency overrides in incident reports

---

## Related Files

- **Development Process**: `DEVELOPMENT_PROCESS.md` - Phase 0 (Branch) and Phase 6 (PR & Merge)
- **Git Workflow**: `docs/development/git-workflow.md` - Detailed workflow guide
- **Auto-Approve Workflow**: `.github/workflows/auto-approve.yml` - CI automation
- **Copilot Instructions**: `.github/copilot-instructions.md` - Internal dev guidelines (not in git)

---

## FAQ

**Q: Why disable auto-merge for docs?**  
A: Even docs need review for accuracy, completeness, and to catch accidental information exposure.

**Q: Can bot approval count as human review?**  
A: Technically yes (it satisfies the "1 approval" requirement), but human review is still recommended.

**Q: What if CI is broken and blocking merges?**  
A: Fix CI first, or temporarily disable the specific failing check in branch protection (document why).

**Q: Can we use auto-merge for dependency updates?**  
A: Yes, Dependabot PRs can be auto-merged if they pass CI and only update dependencies. Configure separately.

---

**Last Updated**: January 12, 2026  
**Status**: Branch protection recommended but requires manual UI configuration  
**Auto-merge**: DISABLED (requires manual merge even for docs)
