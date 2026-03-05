#!/usr/bin/env bash
set -euo pipefail

CWD="$(pwd)"


VENV_DIR="$CWD/.venv"

if [[ ! -d "$VENV_DIR" ]]; then
  echo "Creating virtual environment in $VENV_DIR …"
  python3.12 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"


python -m pip install --upgrade pip -q
pip install -e $CWD
