# 2026 Head to Head Heftystrong

Interactive analytics dashboard, real-time scoreboard, historical records, and automated Discord bot for the **2026 Head to Head Heftystrong** fantasy baseball league (ESPN League ID: `130215`).

🌐 **Live Dashboard**: [https://dsellinger-braves.github.io/2026-head-to-head](https://dsellinger-braves.github.io/2026-head-to-head)

---

## Features

- **Live Scoreboard**: Real-time category tracking, live active matchup scores, and active roster lookups.
- **Highlights & Top Performers**: Daily batting and pitching category leaders.
- **Standings & Progression**: Cumulative season performance, category win/loss matrices, and historical trend charts.
- **Owner Disparities & History**: Head-to-head owner records, multi-season history, and trade analytics.
- **Discord Bot**: 24/7 slash-command bot hosted on Railway (`/live`, `/standings`, `/roster`, `/projections`).
- **AI Daily Recaps**: Automated daily editorial summaries powered by Google Gemini and posted directly to Discord via GitHub Actions.

---

## Tech Stack & Architecture

- **Frontend**: React 19, Vite, Tailwind CSS, Recharts, `idb-keyval` (IndexedDB caching).
- **Backend & Database**: Supabase PostgreSQL (`player_daily_stats`, `transactions`, `historical_data`).
- **Data Scrapers**: Python 3.11+, Requests, Pandas, Supabase client.
- **Bots & AI**: `discord.py`, Google Gemini (`google-genai` / `google-generativeai`).
- **Hosting & CI/CD**: GitHub Pages (`deploy.yml`), Railway (Discord Bot), GitHub Actions (Scheduled scrapers).
- **AI Tooling**: Composio & Antigravity MCP integration with project-specific skills (`.agents/`).

---

## Project Structure

```text
├── .agents/                 # AI Agent customizations, skills, and Composio configs
│   ├── plugins/composio/    # Composio MCP plugin configuration
│   └── skills/              # Reusable agent runbooks (github-flow, supabase-ops, frontend-deploy)
├── .github/workflows/       # GitHub Actions CI/CD (scrapers, recap bot, deploy)
├── src/                     # React dashboard frontend
│   ├── views/               # Dashboard view components (LiveScoreboard, Highlights, etc.)
│   ├── components/          # Reusable UI components
│   └── supabaseClient.js    # Supabase JS client configuration
├── active-stats-pull.py     # Ingests daily player stats from ESPN API to Supabase
├── transaction-scraper.py   # Ingests trades, waivers, and free-agent adds/drops
├── projections.py           # Calculates rest-of-season and final standings projections
├── discord-bot.py           # 24/7 Discord bot service (Railway)
├── discord-daily-recap.py   # Daily Gemini AI recap generator
├── check_schema.py          # Verifies active Supabase table columns
├── check_types.py           # Verifies Supabase column data types
└── AGENTS.md                # Detailed system blueprint and developer guidelines
```

---

## Getting Started

### 1. Frontend Development

```bash
# Install dependencies
npm install

# Start local Vite development server
npm run dev

# Build for production
npm run build

# Deploy to GitHub Pages
npm run deploy
```

### 2. Python Environment & Scrapers

```bash
# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Verify database connection
python check_schema.py
```

### Environment Variables (`.env`)

```env
VITE_SUPABASE_URL=https://wczdkcdqgtzlsbssogoz.supabase.co
VITE_SUPABASE_ANON_KEY=your-supabase-anon-key
SUPABASE_URL=https://wczdkcdqgtzlsbssogoz.supabase.co
SUPABASE_KEY=your-supabase-service-or-anon-key
DISCORD_BOT_TOKEN=your-discord-bot-token
GEMINI_API_KEY=your-gemini-api-key
ESPN_S2=your-espn-s2-cookie
ESPN_SWID=your-espn-swid-cookie
```

---

## AI Agent & Developer Guidelines

For agents and contributors working on this codebase:
- Review [AGENTS.md](AGENTS.md) for full system specifications, ESPN stat ID mappings, and table schemas.
- Reusable runbooks are available in `.agents/skills/`:
  - [`github-flow`](.agents/skills/github-flow/SKILL.md): Branching and Composio PR creation.
  - [`supabase-ops`](.agents/skills/supabase-ops/SKILL.md): Database validation and safe migrations.
  - [`frontend-deploy`](.agents/skills/frontend-deploy/SKILL.md): Building and deploying the dashboard.
