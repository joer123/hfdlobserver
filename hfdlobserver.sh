#!/usr/bin/env bash
# hfdlobserver.sh
# copyright Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
if [[ -r "${SCRIPT_DIR}/venv/bin/activate" ]] ; then
    VENV="${SCRIPT_DIR}/venv"
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
# pip install -q -q -q --exists-action i -r requirements.txt
case "${1}" in
    "configure")
        exec python3 "${SCRIPT_DIR}/configure.py"
        ;;
    "requirements")
        exec pip install -r "${SCRIPT_DIR}/requirements.txt"
        ;;
    *)
        export PYTHONPATH="${SCRIPT_DIR}/src"
        exec python3 "${SCRIPT_DIR}/src/hfdlobserver.py" "${@}"
esac
