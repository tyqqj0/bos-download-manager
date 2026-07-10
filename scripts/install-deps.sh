#!/bin/bash
# scripts/install-deps.sh — Run on each worker to install dependencies
set -euo pipefail

echo "Installing system deps..."
apt-get update -qq && apt-get install -y -qq aria2

echo "Installing Python deps..."
cd /root/code/bos-download-manager
pip install -e ".[web]" 2>&1 | tail -3

echo "Verifying..."
python3 -c "import temporalio; print(f'temporalio {temporalio.__version__}')"
aria2c --version | head -1
echo "Done."
