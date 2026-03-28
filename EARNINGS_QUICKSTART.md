# Quick Start: Earnings Analysis Pipeline

## Status
✅ **IMPLEMENTATION COMPLETE** — All code is in place and ready to deploy

## What Was Done
- ✅ Created `backend/pipeline/earnings_ingest.py` — Automated earnings analysis module
- ✅ Updated `backend/dags/stock_pipeline_dag.py` — Integrated earnings_ingest task
- ✅ Enhanced `frontend/pages/02_earnings_summary.py` — Improved UI
- ✅ Verified database schema — `public.earnings_summaries` table exists

## How It Works
```
NSE API Data (extract.py)
  ↓
Fundamentals in Database (bronze.raw_fundamentals)
  ↓
earnings_ingest Task (BART + FinBERT)
  ├─ Reads fundamentals
  ├─ Generates narrative
  ├─ Summarizes with BART
  ├─ Analyzes sentiment with FinBERT
  ↓
Results in Database (public.earnings_summaries)
  ↓
Streamlit Dashboard (02_earnings_summary.py)
```

## Deployment

### Step 1: Start Docker Containers
```bash
cd d:/NSE_Data
docker-compose up -d
```

Wait for containers to start (~30 seconds):
```bash
docker-compose logs airflow-scheduler | head -20
```

### Step 2: Verify DAG Loaded
Check that earnings_ingest task is recognized:
```bash
docker-compose logs airflow-scheduler | grep -i "earnings\|parsed"
```

Expected output: DAG `stock_pipeline` with 9 tasks (including `earnings_ingest`)

### Step 3: Trigger DAG Manually
Visit Airflow UI: http://localhost:8080
- Login: airflow / airflow
- Find `stock_pipeline` DAG in the list
- Click the play button (Trigger DAG)
- Wait for execution

### Step 4: Monitor Task Execution
In Airflow UI:
- Click `stock_pipeline` DAG name
- In the DAG runs table, click the latest run
- View task details → find `earnings_ingest` task
- Click "earnings_ingest" → view logs

Expected logs:
```
Starting earnings_ingest for N symbols...
Processing INFY...
Stored earnings for INFY (date=..., sentiment=...)
...
earnings_ingest complete: N summaries stored/updated
```

### Step 5: Verify Results
Check database directly:
```bash
# From host machine
docker exec -it nse-data-postgres psql -U nse_admin -d nse_data -c \
  "SELECT COUNT(*) as total_summaries, COUNT(DISTINCT ticker) as companies FROM public.earnings_summaries;"
```

Expected: `total_summaries > 0, companies > 0`

### Step 6: View Streamlit Dashboard
Visit: http://localhost:8501/02_earnings_summary
- Should show earnings summaries (NOT "No earnings found")
- Can filter by ticker
- Can expand summaries to read full text
- Can export as CSV

---

## Troubleshooting

### Problem: DAG doesn't parse
**Check:** `docker-compose logs airflow-scheduler | grep -i "error\|failed"`
**Fix:** Ensure no Python syntax errors in `backend/dags/stock_pipeline_dag.py`

### Problem: earnings_ingest task fails
**Check:** Task logs in Airflow UI for specific error
**Check database:** Are fundamentals in `bronze.raw_fundamentals`?
```bash
docker exec -it nse-data-postgres psql -U nse_admin -d nse_data -c \
  "SELECT COUNT(*) FROM bronze.raw_fundamentals;"
```

### Problem: No earnings data in dashboard
**Check:**
1. Did extract_prices task complete successfully?
2. Did earnings_ingest task run and complete?
3. Is data in public.earnings_summaries?

```bash
docker exec -it nse-data-postgres psql -U nse_admin -d nse_data -c \
  "SELECT * FROM public.earnings_summaries LIMIT 5;"
```

### Problem: API rate limits (HuggingFace)
**Note:** earnings_ingest respects the 80 calls/day budget
**Check:** If exceeded, wait for next hour or increase HF_API_QUOTA

---

## Emergency Rollback

If earnings_ingest causes issues, temporarily disable:

Edit `backend/dags/stock_pipeline_dag.py`:
- Comment out lines that depend on earnings_ingest
- Redeploy

```python
# Temporarily disable earnings_ingest dependency:
# _ = t2_extract_prices >> [t4_transform_prices, t3b_earnings_ingest]
# Instead, use only transform_prices:
_ = t2_extract_prices >> t4_transform_prices
```

---

## Key Files

| Path | Purpose |
|------|---------|
| `backend/pipeline/earnings_ingest.py` | Earnings analysis logic |
| `backend/dags/stock_pipeline_dag.py` | Airflow DAG definition |
| `backend/pipeline/db_init.py` | Database schema |
| `frontend/pages/02_earnings_summary.py` | Streamlit UI |
| `IMPLEMENTATION_SUMMARY.md` | Detailed technical docs |

---

## Expected Hourly Schedule

Every hour at :00 UTC:
1. db_init ensures schemas exist
2. extract_prices fetches NSE data
3. earnings_ingest analyzes fundamentals ← **NEW**
4. transform_prices cleans price data
5. transform_news processes news
6. load_gold consolidates results
7. create_views refreshes analytics
8. validate_schema checks integrity

Total runtime: ~5-10 minutes per day

---

## Support

For detailed technical information, see: `IMPLEMENTATION_SUMMARY.md`
