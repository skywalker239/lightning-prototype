#!/bin/bash
cd /home/skywalker/deploy
HOSTID=`./hostid.py`
[ $HOSTID == "0" ] && exit
killall -9 test_ring_responder
rm log
LOG_STDOUT=1 LOG_TRACEMASK="lightning:.*" ./test_ring_responder new_config.json `./hostid.py` > log &
echo "Started responder for host `./hostid.py`"

