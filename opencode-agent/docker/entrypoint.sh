#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "opencode" ]]; then
  python3 /opt/setup/configure-systems.py
fi
exec "$@"
