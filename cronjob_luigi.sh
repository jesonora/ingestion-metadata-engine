#!/bin/bash

export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export LUIGI_CONFIG_PATH=/opt/workspace/ingestion-metadata-engine/config/luigi.cfg

# Set up a cron job to run the Python script every minute
(crontab -l ; echo "* * * * * python3 -m luigi --module main TriggerPipeline --local-scheduler >> /opt/workspace/ingestion-metadata-engine/logfile.log 2>&1") | crontab -

echo "Python script scheduled to run every minute."