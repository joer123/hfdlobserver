#!/bin/sh
cd $HOME \
&& sudo apt install git \
&& git clone https://github.com/hfdl-observer/hfdlobserver888 \
&& cd hfdlobserver888 \
&& ./install.sh
