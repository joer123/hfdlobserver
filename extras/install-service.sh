#!/bin/bash
# extras/install-service.py
# copyright 2024 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
sudo cp "${SCRIPT_DIR}/etc-default-hfdlobserver888" /etc/default/hfdlobserver888
sed -e "s:[$]USER:${USER}:g" -s":[$]HOME:${HOME}:g"< "${SCRIPT_DIR}/hfdlobserver888.service" \
| sudo tee /etc/systemd/system/hfdlobserver888.service
sudo systemctl enable hfdlobserver888
echo "Systemd unit installed and enabled. You can start it now with `sudo systemctl start hfdlobserver888`"
