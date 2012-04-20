#!/bin/bash
cd ${HOME}/deploy
HOSTID=`./hostid.py`
[ $HOSTID == "0" ] && exit
killall -9 test_ring_acceptor
rm log
./test_ring_acceptor new_config.json `./hostid.py` > log &
echo "Started acceptor for host `./hostid.py`"

