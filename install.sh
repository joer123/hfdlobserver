# bootstrap install
# basic requirements.
export NEWT_COLORS='root=,black'
sudo apt install -y whiptail python3 python3-venv git

# create virtualenvironment
pushd $HOME
[[ -d .virtualenvs ]] || mkdir .virtualenvs
VENV="$HOME/.virtualenvs/hfdlobserver888"
python -m venv "${VENV}"
source "${VENV}/bin/activate"
popd
pip install -r requirements.txt
pushd $HOME

# download kiwiclient
git clone https://github.com/jks-prv/kiwiclient.git

# download and install dumphfdl
# dependencies
sudo apt install -y \
    build-essential cmake pkg-config libglib2.0-dev libconfig++-dev libliquid-dev libfftw3-dev \
    zlib1g-dev libxml2-dev libjansson-dev

# libacars
git clone https://github.com/szpajder/libacars
pushd libacars \
&& mkdir build \
&& cd build \
&& cmake ../
make \
&& sudo make install \
&& sudo ldconfig
popd

# libstatsd
git clone https://github.com/romanbsd/statsd-c-client.git
pushd statsd-c-client \
make \
&& sudo make install \
&& sudo ldconfig
popd

# dumphfdl
git clone https://github.com/szpajder/dumphfdl
pushd dumphfdl
mkdir build \
&& cd build \
&& cmake ../
make \
&& sudo make install \
&& sudo ldconfig
popd

# ask questions for settings.yaml
# 1. Stations:
#   A. provide a list
#   B. provide lat/long, and produce a list

# 2. Select directory for packet logging (or disable)
# 3. Airframes Station ID
# 4. Address and Port for Web-888.
python3 configure.py
# Write settings file
cp -i settings.yaml.new settings.yaml
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
# popd
