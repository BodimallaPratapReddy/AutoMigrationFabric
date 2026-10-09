#!/usr/bin/env bash
set -euo pipefail
export PATH="/home/pratap/.nvm/versions/node/v22.19.0/bin:/home/pratap/.opencode/bin:$PATH"
export ABAP_SAP_URL="https://s4han2022.demo.com:8177"
read -r -p "SAP username: " ABAP_SAP_USER
read -r -s -p "SAP password: " ABAP_SAP_PASSWORD
printf '\n'
export ABAP_SAP_USER ABAP_SAP_PASSWORD
cd /mnt/c/Users/bodim/Desktop/Projects/opencode/test-skill-ddic-5
/home/pratap/.opencode/bin/opencode service restart
exec /home/pratap/.opencode/bin/opencode
