#!/bin/bash
# backend/services/start_realtime.sh
# Start the real-time price update service

echo "Starting NSE Real-Time Price Service..."
echo "Updates: Every 1 second during market hours (9:15 AM - 3:30 PM IST)"
echo "Press Ctrl+C to stop"
echo ""

cd "$(dirname "$0")/../../" || exit 1

# Run the Python service
python -m backend.services.realtime_price_service
