#!/bin/bash

# Configuration
REMOTE_USER="azureuser"
REMOTE_HOST="20.15.226.138"
REMOTE_PATH="/home/azureuser/etls/"
LOCAL_DIR="combined"

# Command to copy the folder using rsync
# This will prompt for a password by default.
# -a: archive mode (preserves permissions, times, etc.)
# -v: verbose
# -z: compress during transfer
rsync -avz "$LOCAL_DIR" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}"

# --- AUTOMATION WITH PASSWORD ---
# If you want to put the password in this script (NOT RECOMMENDED for security),
# you can use 'sshpass'. You would need to install sshpass first.
#
# 1. Install sshpass (e.g., brew install sshpass)
# 2. Uncomment the lines below and replace 'YOUR_PASSWORD_HERE'
#
# export SSHPASS='YOUR_PASSWORD_HERE'
# sshpass -e rsync -avz "$LOCAL_DIR" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}"
