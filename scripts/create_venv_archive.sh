#!/bin/bash
set -e

python3.8 -m venv .temp_venv
source .temp_venv/bin/activate

python -m pip install --upgrade pip
python -m pip install wheel
python -m pip install -r requirements-prod.txt
python -m pip install venv-pack

mkdir -p venvs
venv-pack -o venvs/venv.tar.gz

deactivate
rm -rf .temp_venv
