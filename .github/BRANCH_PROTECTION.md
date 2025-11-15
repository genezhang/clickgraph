# Branch Protection Rules Configuration
# This file documents the recommended branch protection rules for auto-approve functionality

## Required Status Checks
# Configure these in GitHub: Settings → Branches → Branch protection rules
# For branch: main

### Required status checks:
- build (from ci.yml)
- Run tests (from ci.yml)

### Required approvals: 1
# This allows auto-merge to work after CI passes

### Additional settings:
- ✅ Require branches to be up to date before merging
- ✅ Include administrators
- ✅ Restrict pushes that create matching branches
- ✅ Allow auto-merge
- ✅ Automatically delete head branches

## Auto-Merge Conditions
The auto-merge workflow will automatically merge PRs that:
1. Pass all required CI checks
2. Have at least 1 approval (can be from auto-approve bot)
3. Only contain safe changes (docs, config, formatting)

## Manual Configuration Steps

1. **Enable auto-merge for the repository:**
   - Go to Settings → General → Pull Requests
   - Check "Allow auto-merge"

2. **Configure branch protection:**
   - Go to Settings → Branches → Branch protection rules
   - Add rule for `main` branch
   - Enable required status checks
   - Set required approvals to 1
   - Enable auto-merge

3. **Set up Dependabot:**
   - The `.github/dependabot.yml` file is already configured
   - Dependabot will auto-merge minor/patch updates

## Testing Auto-Approve
To test the auto-approve functionality:

1. Create a PR with only documentation changes
2. Wait for CI to pass
3. The auto-approve workflow should approve it automatically
4. If auto-merge is enabled, it should merge automatically

