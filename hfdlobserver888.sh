#!/bin/bash
# hfdlobserver888.sh
# copyright 2024 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
VENV="$HOME/.virtualenvs/hfdlobserver888"
if [[ ! -r "${VENV}/bin/activate" ]] ; then
    echo "** The virtual environment does not seem to be present. Did install.sh run successfully?"
    exit 1;
fi
source "${VENV}/bin/activate"
export PYTHONPATH="${SCRIPT_DIR}/src"
exec python3 "${SCRIPT_DIR}/src/main.py" "${@}"
