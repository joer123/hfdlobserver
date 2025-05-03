#!/usr/bin/env bash
if [[ "${1}" == '--help' ]] ; then
    echo 'This script is only needed if you installed this application while it was known as "hfdlobserver888".'
    echo 'It installs additional dependencies, and provides a backwards compatible link for the main script.'
    echo ' '
    echo 'The app should not need to alter any systemd service files, or the name of the install directory.'
    echo ' '
    echo 'You will need to reconfigure the settings.yaml file for the new configuration structure. If you have a'
    echo 'simple installation, running "./hfdlobserver.sh configure", then copying the new configuration file over the'
    echo 'old one should be sufficient. If you have custom configuration, you will have to port that over manually.'
    echo 'Join the HFDL channel on the Airframes Discord (https://discord.gg/airframes) if you need assistance.'
    echo 'The changes are not complicated, but are not automated.'
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

if [[ ! -r "hfdlobserver888.sh" ]] ; then
    echo "installing backwards compatible symlink"
    ln -s "hfdlobserver.sh" "hfdlobserver888.sh"
fi
echo 'Migration tasks completed.'
echo ' '
echo 'You may need to reconfigure the settings.yaml file for the new configuration structure. If you have a'
echo 'simple installation, running "./hfdlobserver.sh configure", then copying the new configuration file over the'
echo 'old one should be sufficient. If you have custom configuration, you will have to port that over manually.'
echo 'Join the HFDL channel on the Airframes Discord (https://discord.gg/airframes) if you need assistance.'
echo 'The changes are not complicated, but are not automated.'
