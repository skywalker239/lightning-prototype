#!/bin/bash
HOSTNAME="mcast.`hostname`"
PING_REQ_PORT=23339
RING_REQ_PORT=23338
PING_REP_PORT=23337
RING_REP_PORT=23336
MULTICAST_GROUP="239.3.0.1"
RESPONDER="/home/skywalker/deploy/test_ring_responder"
LOG_STDOUT=1 LOG_TRACEMASK="lightning:.*" ${RESPONDER} ${HOSTNAME} ${MULTICAST_GROUP} ${PING_REQ_PORT} ${PING_REP_PORT} ${RING_REQ_PORT} ${RING_REP_PORT}

