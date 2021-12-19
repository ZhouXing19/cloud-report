#!/bin/bash

set -ex

rm -rf newnetperf
mkdir newnetperf

cd newnetperf
git clone https://github.com/ZhouXing19/netperf.git
cd netperf
sudo apt-get -y install  automake texinfo librrd-dev libpython3-dev python3-pip net-tools
pip install rrdtool
./autogen.sh
./configure --enable-burst --enable-demo --enable-histogram
sudo make install

sleep 3
echo "finished make install"

cd ../..
pwd

sudo chmod -R 777 newnetperf/

export PATH=$PATH:/home/ubuntu/newnetperf/netperf/doc/examples
cd newnetperf/netperf/doc/examples

chmod +x runemomniaggdemo.sh find_max_burst.sh

sudo sysctl -w net.ipv4.tcp_rmem="4096        131072  32000000"
sudo sysctl -w net.ipv4.tcp_wmem="4096        16384   32000000"

touch ~/load_success


