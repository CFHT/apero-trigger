#!/usr/bin/env bash

function activate_env() {
  environment=$1
  echo "Using environment $environment"
  source /data/spirou/apero/venv/bin/activate
  source /data/spirou/apero/config/${environment}/${environment}.bash.setup
  export PYTHONUNBUFFERED=1
}

if [ $# -ne 1 ]; then
  echo "Must specify realtime environment to use"
  exit 1
else
  case "$1" in
  quicklook)
    activate_env "$1"
    steps="preprocess calibrations extract leak fittellu ccf products distribute"
    /data/spirou/apero/trigger/full_trigger.py realtime --processes 6 --steps ${steps}
    ;;
  snr)
    activate_env "$1"
    steps="preprocess calibrations snronly distraw database"
    /data/spirou/apero/trigger/full_trigger.py realtime --processes 4 --steps ${steps}
    ;;
  *)
    echo "Supported realtime environments are quicklook or snr"
    ;;
  esac
fi
