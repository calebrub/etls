#!/bin/bash

# Configuration
REMOTE_USER="azureuser"
REMOTE_HOST="20.15.226.138"
REMOTE_PATH="/home/azureuser/etls/"
LOCAL_DIR="leenxa"

# Command to copy the folder using rsync
# -a: archive mode (preserves permissions, times, etc.)
# -v: verbose
# -z: compress during transfer
export SSHPASS='84pVrZ6c5XVwF9dnl45jd'
sshpass -e rsync -avz \
  --exclude 'csv_files' \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  --exclude 'logs' \
  "$LOCAL_DIR" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}"
