# Post-Deployment Verification Checklist

## ✅ Implementation Complete

Use this checklist to verify the earnings analysis pipeline is working correctly after deployment.

---

## Phase 1: Docker & Airflow (5-10 min)

- [ ] **Docker containers started successfully**
  ```bash
  docker-compose ps
  # Should show: postgres, chromadb, airflow-scheduler, airflow-webserver, airflow-worker all UP
  ```

- [ ] **Airflow UI accessible**
  - Visit http://localhost:8080
  - Login: airflow / airflow
  - Should see home page with DAGs

- [ ] **stock_pipeline DAG appears in list**
  - On Airflow home, scroll to DAGs
  - Should see `stock_pipeline` tagged with: nse, finance, etl, sprint4

- [ ] **DAG shows 9 tasks** (not 8)
  - Click on `stock_pipeline`
  - In graph view, should see tasks: db_init, extract_prices, news_ingest, earnings_ingest← **NEW**, transform_prices, transform_news, load_gold, create_views, validate_schema

- [ ] **Scheduler logs show no errors**
  ```bash
  docker-compose logs airflow-scheduler | grep -i "error" | head -10
  # Should be empty or unrelated errors
  ```

---

## Phase 2: DAG Structure (2-5 min)

- [ ] **earnings_ingest task wired correctly**
  - Graph view: extract_prices → earnings_ingest
  - Graph view: earnings_ingest → load_gold

- [ ] **All task dependencies correct**
  ```
  db_init
    ├─ extract_prices ─┬─ transform_prices ─┐
    └─ news_ingest ────┼─ earnings_ingest ──┼─ load_gold
                       └─ transform_news ───┘
  ```

- [ ] **DAG configured for hourly runs**
  - Click `stock_pipeline` → `DAG Details`
  - "Schedule Interval" should show: `0 * * * *` (hourly)

---

## Phase 3: Manual Trigger & Monitoring (10-15 min)

- [ ] **Manually trigger DAG**
  - On `stock_pipeline` page, click "Trigger DAG" button
  - Should see confirmation message

- [ ] **Monitor DAG run**
  - Wait 2-3 minutes for tasks to start
  - Click on the latest DAG run
  - Observe tasks transitioning: running → success

- [ ] **earnings_ingest task completes**
  - Task status: `SUCCESS` (green)
  - Duration: 30s - 5m depending on symbols count

- [ ] **earnings_ingest task logs contain success messages**
  - Click `earnings_ingest` task → "Log" tab
  - Should see messages like:
    ```
    Starting earnings_ingest for N symbols...
    Processing INFY...
    Stored earnings for INFY (date=..., sentiment=...)
    earnings_ingest complete: N summaries stored/updated
    ```

- [ ] **All tasks complete successfully**
  - All 9 tasks should be green (SUCCESS)
  - DAG run status: SUCCESS

---

## Phase 4: Database Verification (5 min)

- [ ] **earnings_summaries table exists**
  ```bash
  docker exec -it nse-data-postgres psql -U nse_admin -d nse_data -c \
    "\dt public.earnings_summaries"
  # Should show: public | earnings_summaries | table
  ```

- [ ] **Table has data**
  ```bash
  docker exec -it nse-data-postgres psql -U nse_admin -d nse_data -c \
    "SELECT COUNT(*) as total_rows FROM public.earnings_summaries;"
  # Should show: total_rows > 0
  ```

- [ ] **Data has expected columns**
  ```bash
  docker exec -it nse-data-postgres psql -U nse_admin -d nse_data -c \
    "SELECT ticker, report_date, created_at FROM public.earnings_summaries LIMIT 3;"
  # Should show: ticker (e.g., INFY), report_date, created_at timestamp
  ```

- [ ] **Summaries contain text**
  ```bash
  docker exec -it nse-data-postgres psql -U nse_admin -d nse_data -c \
    "SELECT ticker, LENGTH(summary) as summary_length FROM public.earnings_summaries LIMIT 3;"
  # Should show: summary_length > 100 (BART summaries are 50-200 tokens)
  ```

- [ ] **Recent data present**
  ```bash
  docker exec -it nse-data-postgres psql -U nse_admin -d nse_data -c \
    "SELECT MAX(created_at) as latest_update FROM public.earnings_summaries;"
  # Should show: created_at from current date/time
  ```

---

## Phase 5: Streamlit Dashboard (5 min)

- [ ] **Streamlit app accessible**
  - Visit http://localhost:8501
  - Should load without errors

- [ ] **02_earnings_summary page loads**
  - Click "Earnings Analysis" in sidebar
  - Page title: "💼 Earnings Analysis" or similar

- [ ] **No more "No earnings summaries found" error**
  - Page should display actual earnings data (not the empty state message)

- [ ] **Metrics display correctly**
  - Three metrics shown: Total Summaries, Companies, Latest Update
  - Numbers should be > 0 and reasonable

- [ ] **Earnings summaries visible**
  - "Earnings Insights" section shows multiple expandable cards
  - Each card shows: 🏢 Ticker, 📅 Report Date, 📝 Source
  - First card expanded by default showing full summary text

- [ ] **Ticker filter works**
  - Sidebar → "Select Ticker" dropdown shows list of stocks
  - Select a ticker → page filters to only that ticker
  - Select "All Tickers" → shows all again

- [ ] **CSV export works**
  - Sidebar → "Download as CSV" button
  - Click to download file
  - File contains all summaries data

---

## Phase 6: Data Quality (5 min)

- [ ] **Summary text is meaningful**
  - Click expander on one earnings summary
  - Read the summary text
  - Should be coherent financial analysis (not gibberish)

- [ ] **Timestamps are correct**
  - Created_at dates should be recent (today or yesterday)
  - No dates from 2024 or far future

- [ ] **All symbols processed**
  - Check that multiple different tickers have summaries
  - Not just one or two symbols

---

## Phase 7: Scheduled Execution (verify after 1 hour)

- [ ] **Next hourly DAG run triggered automatically**
  - After 1 hour, visit Airflow UI
  - `stock_pipeline` DAG should have a new run
  - Status: running or completed

- [ ] **earnings_ingest runs again**
  - Latest DAG run → earnings_ingest task present
  - Task status: SUCCESS

- [ ] **More earnings data in database**
  ```bash
  docker exec -it nse-data-postgres psql -U nse_admin -d nse_data -c \
    "SELECT COUNT(*) as total FROM public.earnings_summaries; SELECT MAX(created_at) FROM public.earnings_summaries;"
  # Count should match previous (or increase if new symbols)
  # MAX(created_at) should be from this hour's run
  ```

---

## Troubleshooting Reference

| Issue | Check | Fix |
|-------|-------|-----|
| earnings_ingest missing from DAG | DAG has 8 tasks instead of 9 | Redeploy DAG file |
| earnings_ingest task fails | Check task logs in Airflow | See EARNINGS_QUICKSTART.md |
| No data in earnings_summaries table | Check if extract_prices completed | Verify bronze.raw_fundamentals has data |
| Streamlit shows old data | Check last created_at timestamp | Manually trigger DAG to refresh |
| "No earnings summaries found" still shows | Refresh browser cache | Clear browser cookies, retry |

---

## Success Criteria

✅ **All Boxes Checked = System is Working**

- [ ] DAG has 9 tasks including earnings_ingest
- [ ] DAG runs successfully every hour
- [ ] earnings_ingest completes without errors
- [ ] public.earnings_summaries table has data
- [ ] Streamlit dashboard displays summaries
- [ ] No "No earnings summaries found" message

---

## Sign-Off

**Date Verified:** ___________
**Verified By:** ___________
**Status:** ☐ Ready for Production  ☐ Issues Found (see notes)

**Notes:**
```
[Space for verification notes]
```

---

## Emergency Contact

If critical issues arise:
1. Check Docker logs: `docker-compose logs`
2. Check Airflow scheduler: `docker-compose logs airflow-scheduler`
3. See EARNINGS_QUICKSTART.md troubleshooting section
4. Review IMPLEMENTATION_SUMMARY.md for technical details
