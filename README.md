# Skylark Drones — Monday.com BI Agent

A production-ready AI Business Intelligence agent with live Monday.com integration, true agentic tool-calling, and a refined dark UI.

---

## Architecture

```
Browser (HTML/JS/CSS)
      │  POST /api/query + credentials in headers
      ▼
FastAPI backend  (main.py)
      │  BIAgent.query()
      ▼
Claude claude-opus-4-5  ──tool_use──►  MondayClient  ──GraphQL──►  Monday.com
      │                 ◄──results───                ◄────────────
      ▼
  Markdown answer + full trace JSON
```

**Key design decisions:**
| Concern | Choice | Why |
|---|---|---|
| AI model | Claude Opus 4.5 | Best multi-step reasoning for BI |
| Tool loop | True Anthropic tool_use | Claude decides what/when to fetch |
| API | Monday.com GraphQL v2 | Cursor pagination, no data loss |
| Backend | FastAPI (async) | Non-blocking concurrent API calls |
| Auth | Header-pass-through | No server restart to change boards |
| Data cleaning | Dedicated `data_cleaner.py` | Sentinel rows, ₹ parsing, sector normalise |
| Hosting | Render.com free tier | Zero-config, auto-HTTPS |

---

## Quickstart (6 minutes)

### 1. Import data into Monday.com

```bash
pip install requests pandas openpyxl
python scripts/monday_import.py \
    --api-key   YOUR_MONDAY_TOKEN \
    --workspace YOUR_WORKSPACE_ID \
    --deals-file Deal_funnel_Data.xlsx \
    --wo-file    Work_Order_Tracker_Data.xlsx
```
Copy the two board IDs printed at the end.

Your Monday token: **Admin → API → Generate Token**  
Your Workspace ID: visible in the URL `monday.com/workspaces/XXXXX`

---

### 2. Run locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Copy and fill in credentials
cp .env.example .env

uvicorn backend.main:app --reload --port 8000
```

Open **http://localhost:8000** — the backend serves the frontend.

---

### 3. Deploy to Render.com

1. Push repo to GitHub
2. Go to **render.com → New → Web Service**
3. Connect your repo
4. Set **Start command**: `uvicorn backend.main:app --host 0.0.0.0 --port $PORT`
5. Set **Environment variables**:
   - `ANTHROPIC_API_KEY`
   - `MONDAY_API_KEY` *(optional — can also be passed from the UI)*
   - `DEALS_BOARD_ID`
   - `WORKORDERS_BOARD_ID`
6. Deploy → copy the `https://xxx.onrender.com` URL
7. Open the app, click ⚙ Config, paste your Render URL as Backend URL

---

## Project structure

```
skylark-bi/
├── backend/
│   ├── main.py           ← FastAPI app + credential resolution
│   ├── agent.py          ← True agentic tool-use loop (Claude Opus 4.5)
│   ├── monday_client.py  ← Async GraphQL client + cursor pagination
│   └── data_cleaner.py   ← Normalisation, sentinel-row removal, ₹ parsing
├── frontend/
│   └── index.html        ← Full SPA – no build step
├── scripts/
│   └── monday_import.py  ← XLSX → Monday.com importer
├── requirements.txt
├── render.yaml
└── .env.example
```

---

## Sample queries

- How's our pipeline looking this quarter?
- Which sector has the highest deal value?
- Show me all deals in the negotiation stage
- What's the total accounts receivable?
- Compare Mining vs Renewables pipeline health
- Who are the top performing deal owners?
- What's our collection rate on completed work orders?
- Any deals closing this month with high probability?
