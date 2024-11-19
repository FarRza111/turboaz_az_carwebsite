FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create logs directory
RUN mkdir -p logs

# Make the run script executable
RUN chmod +x run_scraper.sh

# Add crontab file
COPY crontab_entry.txt /etc/cron.d/scraper-cron
RUN chmod 0644 /etc/cron.d/scraper-cron
RUN crontab /etc/cron.d/scraper-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run both cron and tail the logs
CMD cron && tail -f /var/log/cron.log
