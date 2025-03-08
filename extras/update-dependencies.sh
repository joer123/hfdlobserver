#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
HFDL_PATH=$(realpath "${SCRIPT_DIR}/..")
cd "${HFDL_PATH}/"
if [[ -r "${HFDL_PATH}/venv/bin/activate" ]] ; then
    VENV="${HFDL_PATH}/venv"
elif [[ -r "${HOME}/.virtualenvs/hfdlobserver888/bin/activate" ]] ; then
    VENV="${HOME}/.virtualenvs/hfdlobserver888"
else
    VENV="${HOME}/.virtualenvs/hfdlobserver"
fi
if [[ ! -r "${VENV}/bin/activate" ]] ; then
    echo "** The virtual environment does not seem to be present. Did install.sh run successfully?"
    exit 1;
fi
echo "Using virtual environment at ${VENV}"
source "${VENV}/bin/activate"
echo "updating package dependencies"
which sqlite3 > /dev/null || sudo apt install sqlite3
echo "updating Python dependencies"
pip install -r requirements.txt
echo "update complete"
