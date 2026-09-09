---
name: frontend-deploy
description: >-
  Build, test, and deployment procedures for the React 19 / Vite dashboard in the
  2026 Head to Head Heftystrong project.
---

# Frontend Deployment Runbook

This guide covers building, testing, and deploying the React frontend dashboard to GitHub Pages.

---

## 1. Local Development

Run the local Vite development server with Hot Module Replacement (HMR):

```bash
npm run dev
```

Visit `http://localhost:5173` to view the dashboard.

---

## 2. Pre-Deployment Validation

Always verify that the code passes linting and builds cleanly without warnings or errors:

```bash
# Run ESLint
npm run lint

# Build production bundle to dist/
npm run build
```

If any ESLint errors or build issues occur, fix them before proceeding.

---

## 3. Deploying to GitHub Pages

### Option A: Manual CLI Deploy
The project includes `gh-pages` tooling:

```bash
# This automatically runs `npm run build` and pushes dist/ to the gh-pages branch
npm run deploy
```

### Option B: GitHub Actions Deploy
Pushing commits to `main` automatically triggers `.github/workflows/deploy.yml`, which:
1. Checks out the repository.
2. Sets up Node.js.
3. Installs dependencies and builds the project.
4. Deploys the static assets to GitHub Pages.

Live URL: `https://dsellinger-braves.github.io/2026-head-to-head`
