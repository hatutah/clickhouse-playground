#!/usr/bin/env bash
set -euo pipefail
# A repeated compose up must not submit a second consumer job.
if curl -fsS http://jobmanager:8081/jobs/overview | grep -q '"state":"RUNNING"'; then
  echo 'Flink job already running; skipping submission.'
  exit 0
fi
/opt/flink/bin/flink run --detached /opt/flink/usrlib/search-click-transform.jar
