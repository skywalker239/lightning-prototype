#!/bin/bash
cd ${HOME}/deploy
HOSTID=`./hostid.py`
[ $HOSTID == "0" ] && exit
killall -9 test_ring_acceptor
sleep 2
rm log
LOG_STDOUT=1 LOG_DEBUGMASK="lightning:(set_ring_handler|recovery_manager|recovery_connection|tcp_recovery_service)" ./test_ring_acceptor new_config.json `./hostid.py` >& log &
echo "Started acceptor for host `./hostid.py`"

