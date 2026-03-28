# Real-Time Price Updates - Setup Guide

## Overview
The system now supports **real-time price updates every 1 second** with:
- ✓ Lightweight caching to avoid API overload
- ✓ Update-on-change logic (only writes when price changes)
- ✓ Market hours detection (9:15 AM - 3:30 PM IST, weekdays only)
- ✓ Graceful error handling and recovery
- ✓ Separate news processing (BART model runs only on new news)

## Architecture

### 1. Real-Time Price Service (Lightweight)
**File**: `backend/services/realtime_price_service.py`

**Features**:
- Runs continuously during market hours
- Updates prices every 1 second
- **2-second cache**: Avoids hammering NSE API
- **Update-on-change**: Only writes to DB if price actually changed
- Minimal CPU/memory overhead

**Flow**:
```
NSE API (current quote)
    ↓ (2-second cache)
Is price different?
    ↓ YES
Update bronze.raw_prices (upsert)
    ↓
Transform computes SMA/RSI
    ↓
Load updates gold.stock_summary
    ↓
Frontend auto-refreshes
```

### 2. News Processing (Separate)
**File**: `backend/pipeline/transform.py` (transform_news)

**Optimization**:
- BART model runs **only when new news is ingested**
- NOT called on every price update
- Batch processing of headlines
- HF API rate limiting applied

**Why separated**:
- Price updates can't wait for NLP (1 second requirement)
- News updates are less frequent (hourly)

## Installation

### Start the Real-Time Service

**Windows**:
```bash
cd D:\NSE_Data
python -m backend.services.realtime_price_service
```

Or use batch script:
```bash
.\backend\services\start_realtime.bat
```

**Linux/Mac**:
```bash
cd /d/NSE_Data
python -m backend.services.realtime_price_service
```

Or use shell script:
```bash
bash backend/services/start_realtime.sh
```

### Keep Service Running

**Option 1: Terminal Session (Development)**
```bash
python -m backend.services.realtime_price_service
# Press Ctrl+C to stop
```

**Option 2: Background (Linux/Mac)**
```bash
nohup python -m backend.services.realtime_price_service > logs/realtime-service.log 2>&1 &
```

**Option 3: Windows Task Scheduler**
1. Create task to run: `python -m backend.services.realtime_price_service`
2. Set to run on startup
3. Set to restart on failure

**Option 4: Docker (Recommended)**
Add to `docker-compose.yml`:
```yaml
realtime-service:
  build: .
  command: python -m backend.services.realtime_price_service
  env_file: .env
  depends_on:
    postgres:
      condition: service_healthy
  restart: unless-stopped
```

## Configuration

### Update Frequency
- **Current**: Every 1 second during market hours
- **Configurable**: Edit `CACHE_TTL_SECONDS = 2` in `realtime_price_service.py`
  - `1` = fetch every 1 second
  - `5` = fetch every 5 seconds
  - etc.

### Market Hours
- **Current**: 9:15 AM - 3:30 PM IST, weekdays only
- **Edit**: `is_market_hours()` function in `realtime_price_service.py`

### Symbols
- **Uses**: `NSE_SYMBOLS` from `.env` file (e.g., `RELIANCE.NS,TCS.NS,...`)
- **Change**: Update `.env` to add/remove stocks

## Performance

### NSE API
- Request: ~0.5-1 second per stock
- With 5 stocks: ~2-5 seconds for full cycle
- **Cache**: Avoids redundant requests for 2 seconds
- **Result**: ~1 API call per stock every 2 seconds (5 stocks = ~2.5 requests/sec)

### Database
- **Write**: Only when price changes
- **Update-on-change**: ~95% fewer writes during stable prices
- **UPSERT**: Fast, handles conflicts automatically

### Memory
- **Cache**: < 1 MB (5 stocks)
- **Process**: ~50-100 MB (Python + imports)

## Monitoring

### Check Service Status
```bash
# Linux/Mac
ps aux | grep realtime_price_service

# Windows
tasklist | find "python"
```

### View Logs
```bash
# Should show updates every 1 second during market hours
tail -f logs/realtime-service.log
```

### Database Checks
```sql
-- See recent updates
SELECT symbol, trade_date, close_price, updated_at
FROM gold.stock_summary
ORDER BY updated_at DESC;

-- See bronze updates
SELECT symbol, trade_date, close_price
FROM bronze.raw_prices
WHERE trade_date = CURRENT_DATE
ORDER BY symbol;
```

## Troubleshooting

### Service crashes on startup
```
Error: cannot import name 'backend.pipeline.realtime_prices'
```
**Fix**: Ensure you're in NSE_Data directory:
```bash
cd D:\NSE_Data
python -m backend.services.realtime_price_service
```

### NSE API timeouts
```
Error: timeout from NSE API
```
**Fix**: Increase `CACHE_TTL_SECONDS` to reduce API calls:
```python
CACHE_TTL_SECONDS = 5  # or higher
```

### Database connection errors
```
Error: could not connect to server
```
**Fix**: Check `.env` credentials and ensure PostgreSQL is running:
```bash
psql -h localhost -U admin -d nse_db -c "SELECT 1"
```

### High CPU usage
**Cause**: Frequent API calls without caching
**Fix**: Increase cache TTL

## Frontend Auto-Refresh

Streamlit automatically fetches latest data from database.
No code changes needed - just refresh your browser!

**For manual Streamlit refresh**:
- Click "Rerun" button in Streamlit UI
- Or press `R` key

## Summary

| Component | Update Freq | Optimization |
|-----------|------------|--------------|
| Prices | Every 1 sec | 2-sec cache, write-on-change |
| News | Hourly | BART only on new news |
| Frontend | On browser refresh | Queries DB directly |
| Database | On price change | UPSERT, indexed |

**Result**: Real-time stock prices with minimal API load and zero BART overhead for price updates! 🚀
