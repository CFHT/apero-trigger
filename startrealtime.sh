#!/usr/bin/env bash

environment=snr

echo "Using environment $environment"
source /conda/miniconda3/bin/activate
conda activate apero-env
source /apero/config/${environment}/${environment}.bash.setup
export PYTHONUNBUFFERED=1

echo "Starting realtime trigger"
# steps: preprocess calibrations snronly distraw database
/apero/trigger/online_trigger.py realtime --processes 4
