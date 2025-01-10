#!/usr/bin/env bash
# bootstrap install
# basic requirements.
if [[ $UID = 0 ]] ; then
    echo "This installer is intended to be run as a normal user."
    exit -1
fi
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

export NEWT_COLORS='root=,black'
sudo apt install -y whiptail python3 python3-venv git

# create virtualenvironment
cd $HOME
[[ -d .virtualenvs ]] || mkdir .virtualenvs
VENV="$HOME/.virtualenvs/hfdlobserver888"
python3 -m venv "${VENV}"
if [[ $? != 0 ]] ; then
    echo 'could not create virtual environment install failed; bailing.'
    exit -1
fi

cd "$SCRIPT_DIR"
source "${VENV}/bin/activate"
pip install -r requirements.txt

# download kiwiclient if needed
if [[ -r kiwiclient/kiwirecorder.py ]] ; then
    echo "kiwirecorder is already available, not fetching"
else
    git clone https://github.com/jks-prv/kiwiclient.git
fi

# download and install dumphfdl if needed
if ( which dumphfdl ) then
    echo "dumphfdl appears to be installed. Skipping build."
else
    # dependencies
    sudo apt install -y \
        build-essential cmake pkg-config libglib2.0-dev libconfig++-dev libliquid-dev libfftw3-dev \
        zlib1g-dev libxml2-dev libjansson-dev \
    || exit -1

    # libacars
    ( [[ -d libacars ]] || git clone https://github.com/szpajder/libacars ) \
    && pushd libacars \
    && ( [[ -d build ]] || mkdir build )\
    && cd build \
    && cmake ../ \
    && make \
    && sudo make install \
    && sudo ldconfig \
    && popd
    if [[ $? != 0 ]] ; then
        echo 'libacars install failed; bailing.'
        exit -1
    fi

    # libstatsd
    ( [[ -d statsd-client ]] || git clone https://github.com/romanbsd/statsd-c-client.git ) \
    && pushd statsd-c-client \
    && make \
    && sudo make install \
    && sudo ldconfig \
    && popd
    if [[ $? != 0 ]] ; then
        echo 'statsd-c-client install failed. Statsd will not be available'
    fi

    # dumphfdl
    ( [[ -d dumphfdl ]] || git clone https://github.com/szpajder/dumphfdl ) \
    && pushd dumphfdl \
    && ( [[ -d build ]] || mkdir build )\
    && cd build \
    && cmake ../ \
    && make \
    && sudo make install \
    && sudo ldconfig \
    && popd
    if [[ $? != 0 ]] ; then
        echo 'dumphfdl install failed; bailing.'
        exit -1
    fi
fi

# ask questions for settings.yaml
# 1. Stations:
#   A. provide a list
#   B. provide lat/long, and produce a list
# 2. Select directory for packet logging (or disable)
# 3. Airframes Station ID
# 4. Address and Port for Web-888.
python3 configure.py
# Write settings file
if [[ -r settings.yaml.new ]] ; then
    cp -i settings.yaml.new settings.yaml
fi
# TODO: Write service file?
# good to go.
python3 - << EOF
import whiptail

w = whiptail.Whiptail('Installation', backtitle='HFDL.observer/888: A multi-headed dumphfdl receiver for Web-888 devices')

w.msgbox("""
HFDL.observer/888 is installed and configured!

run './hfdlobserver888.sh' to start. Consult './hfdlobserver888.sh --help' for some basic usage notes.

the file 'settings.yaml.annotated' file contains more complete settings with comments.
You can look at this for aid in manually configurating it for advanced use cases.

To install HFDL.observer/888 as a service, run the 'extras/install-service.sh' script.
""")
EOF

deactivate  # deactivate Venv for now.
