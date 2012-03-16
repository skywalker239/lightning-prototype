#!/bin/bash
cd ${HOME}/deploy
HOSTID=`./hostid.py`
[ $HOSTID == "0" ] && exit
killall -9 test_ring_responder
sleep 2
rm log
LOG_STDOUT=1 LOG_TRACEMASK="lightning:set_ring_handler" LOG_DEBUGMASK="lightning:acceptor_state" ./test_ring_responder new_config.json `./hostid.py` >& log &
echo "Started responder for host `./hostid.py`"

