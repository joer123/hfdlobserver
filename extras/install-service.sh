#!/bin/bash
# extras/install-service.py
# copyright 2024 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
sudo cp "${SCRIPT_DIR}/etc-default-hfdlobserver" /etc/default/hfdlobserver \
&& sed -e "s:[$]USER:${USER}:g" \
    -e "s:[$]HOME:${HOME}:g" \
    -e "s:[$]SCRIPT_DIR:${SCRIPT_DIR}:g" \
    < "${SCRIPT_DIR}/hfdlobserver.service" \
| sudo tee /etc/systemd/system/hfdlobserver.service \
&& sudo systemctl daemon-reload \
&& sudo systemctl enable hfdlobserver \
&& echo "Systemd unit installed and enabled. You can start it now with `sudo systemctl start hfdlobserver`"
