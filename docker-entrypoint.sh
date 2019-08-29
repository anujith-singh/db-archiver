#!/bin/bash
set -e

# Variables expected to be set in the container environment
# - ${worker_cmd} -- Command which will start the app

echo "[ENTRYPOINT] Starting Worker: ${worker_cmd}"
eval ${worker_cmd}
