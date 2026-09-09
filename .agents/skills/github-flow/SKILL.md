---
name: github-flow
description: >-
  Standard GitHub workflow for the 2026 Head to Head Heftystrong repository.
  Covers creating feature branches, verifying changes locally, pushing to remote,
  and using Composio GitHub tools to open pull requests.
---

# GitHub Flow & Pull Request Runbook

Follow this workflow whenever implementing non-trivial changes, bug fixes, or new features in the `2026-head-to-head` repository.

---

## 1. Branch Creation

Always work on an isolated branch rather than committing directly to `main`:

```bash
# Ensure local main is up to date
git checkout main
git pull origin main

# Create a scoped feature or fix branch
git checkout -b feat/description-of-feature
# or
git checkout -b fix/description-of-bug
```

---

## 2. Pre-Commit Verification

Before staging files, run the relevant validation commands:

- **Frontend changes**:
  ```bash
  npm run lint
  npm run build
  ```
- **Backend / Scraper changes**:
  ```bash
  python check_schema.py
  python check_types.py
  ```

Ensure no secrets or unneeded build artifacts (`dist/`, `.env`) are staged.

---

## 3. Commit & Push

Stage only modified files relevant to the task:

```bash
git add <files>
git commit -m "feat(scope): concise description of what changed"
git push -u origin feat/description-of-feature
```

---

## 4. Open Pull Request via Composio

Use Composio's GitHub tool to create a pull request:

```bash
composio execute GITHUB_CREATE_PULL_REQUEST -d '{
  "owner": "dsellinger-braves",
  "repo": "2026-head-to-head",
  "title": "feat: Title of the change",
  "head": "feat/description-of-feature",
  "base": "main",
  "body": "## Summary\n- Bulleted explanation of changes\n\n## Verification\n- Lint and build outputs\n- Manual verification details"
}'
```

Alternatively, open the PR directly via GitHub web UI:
`https://github.com/dsellinger-braves/2026-head-to-head/pull/new/feat/description-of-feature`

---

## 5. Automated CI Checks

Once the PR is opened:
- GitHub Actions workflows in `.github/workflows/` (`deploy.yml`, etc.) will run automated checks.
- Review any workflow runs before merging into `main`.
