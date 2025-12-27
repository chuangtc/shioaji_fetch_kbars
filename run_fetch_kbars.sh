#!/bin/bash

# Change to script directory
cd "$(dirname "$0")"

# Set timezone to Taipei
export TZ='Asia/Taipei'

# Log file with timestamp
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/fetch_kbars_$(date +%Y%m%d_%H%M%S).log"

# Run the Python script and log output
echo "==================================================" >> "$LOG_FILE"
echo "Starting kbar fetch at $(date)" >> "$LOG_FILE"
echo "==================================================" >> "$LOG_FILE"

# Activate virtual environment and run Python script
source .venv/bin/activate
python fetch_and_insert_kbars.py "$@" >> "$LOG_FILE" 2>&1

EXIT_CODE=$?

echo "==================================================" >> "$LOG_FILE"
echo "Finished at $(date) with exit code $EXIT_CODE" >> "$LOG_FILE"
echo "==================================================" >> "$LOG_FILE"

# Keep only last 30 days of logs
find "$LOG_DIR" -name "fetch_kbars_*.log" -mtime +30 -delete

exit $EXIT_CODE
