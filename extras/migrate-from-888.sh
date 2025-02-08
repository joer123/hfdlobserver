#!/usr/bin/env bash
if [[ "${1}" == '--help' ]] ; then
    echo 'This script is only needed if you installed this application while it was known as "hfdlobserver888".'
    echo 'It installs additional dependencies, patches the settings.yaml file, and provides a backwards compatible link for'
    echo 'the main script.'
    echo ' '
    echo 'The app should not need to alter any systemd service files, or the name of the install directory.'
    exit 0;
fi

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
sudo apt install sqlite3
echo "updating Python dependencies"
pip install -r requirements.txt

if [[ -r "settings.yaml" ]] ; then
    echo "ensuring settings key is updated in default settings.yaml"
    sed -i.orig -e "s/^observer888:/observer:/" "settings.yaml"
fi
if [[ ! -r "hfdlobserver888.sh" ]] ; then
    echo "installing backwards compatible symlink"
    ln -s "hfdlobserver.sh" "hfdlobserver888.sh"
fi
echo "Migration tasks completed. You should restart `hfdlobserver.sh` in case there are any peculiarities of your install that need addressing."
