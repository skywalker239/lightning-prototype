#!/bin/bash

CONFIG="config.json"
MASTER="./test_ring_master"
LOG_STDOUT=1 LOG_TRACEMASK="lightning:ring_oracle|lightning:ring_manager" ${MASTER} ${CONFIG}

