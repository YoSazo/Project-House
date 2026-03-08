# House Brain Sim (Neon + React + Scheduler)

This project implements your 3 core components in one repo:
- Digital twin: `grid_cells` state machine in Postgres (Neon-ready)
- Pipeline scheduler: multi-house stage progression + robot cluster assignment
- Simulation runtime: robot/fabricator timing loop with live metrics

## Stack
- Backend: Node.js, Express, WebSocket, pg
- Frontend: React, Vite, React Three Fiber
- Database: PostgreSQL (Neon)

## 1) Configure env

Copy and edit:

```powershell
Copy-Item .env.example .env
Copy-Item web/.env.example web/.env
```

Set `DATABASE_URL` in `.env` to your Neon connection string.

## 2) Install deps

```powershell
npm install
```

## 3) Apply schema + seed

```powershell
npm run db:apply
npm run db:seed
```

## 4) Run app

```powershell
npm run dev
```

- API: `http://localhost:8787`
- UI: `http://localhost:5173`

## API endpoints
- `GET /api/health`
- `GET /api/state`
- `GET /api/metrics/curve`
- `GET /api/metrics/matrix`
- `GET /api/houses/:houseId/grid`
- `POST /api/pipeline/target` with `{ "target_houses": 10 }`
- `POST /api/robots/target` with `{ "target_robots": 18 }`
- `POST /api/experiment/reset` with `{ "target_houses": 12, "target_robots": 18 }`
- `POST /api/tick`

## Experiment flow
1. Set houses and robots with sliders/presets.
2. Click `Reset Experiment Window`.
3. Let it run for a fixed duration.
4. Read:
   - KPI strip (current run)
   - Efficiency curve (houses -> efficiency)
   - Matrix table (houses x robots)

## Fast demo tweaks
- Lower fabrication delay for quick visual progress:
  - set `FABRICATION_SECONDS=5`
- Faster loop:
  - set `SCHEDULER_MS=2000`