#!/bin/bash

# Get the directory where the script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Create logs directory if it doesn't exist
mkdir -p "$DIR/logs"

# Set log file name with timestamp
LOG_FILE="$DIR/logs/scraper_$(date +%Y%m%d_%H%M%S).log"

# Activate virtual environment if you have one
# source /path/to/your/venv/bin/activate

# Run the scraper and log output
echo "Starting scraper at $(date)" >> "$LOG_FILE"
cd "$DIR" && python3 main.py >> "$LOG_FILE" 2>&1
echo "Finished scraper at $(date)" >> "$LOG_FILE"

# Keep only last 30 days of logs
find "$DIR/logs" -name "scraper_*.log" -type f -mtime +30 -delete
