#!/usr/bin/python

import json
import sys

MCAST_SRC_PORT = 23339
MCAST_LISTEN_PORT = 23338
MCAST_REPLY_PORT = 23337
RING_PORT = 23336
UCAST_LISTEN_PORT = 23335

def mcastHost(host):
    return "mcast." + host

def addPort(host, port):
    return host + ":" + str(port)

def loadHosts(filename):
    with file(filename, 'r') as f:
        return json.load(f)

def genHostConfiguration(hosts):
    configuration = []
    for (hostname, dc) in hosts:
        configuration.append([hostname,
                              dc,
                              addPort("0.0.0.0", MCAST_LISTEN_PORT),
                              addPort(mcastHost(hostname), MCAST_REPLY_PORT),
                              addPort(mcastHost(hostname), MCAST_SRC_PORT),
                              addPort(mcastHost(hostname), RING_PORT),
                              addPort(mcastHost(hostname), UCAST_LISTEN_PORT)])
    configuration.append(["LEARNER",
                          "ANY",
                          addPort("0.0.0.0", MCAST_LISTEN_PORT),
                          addPort("0.0.0.0", MCAST_REPLY_PORT),
                          addPort("0.0.0.0", MCAST_SRC_PORT),
                          addPort("0.0.0.0", RING_PORT),
                          addPort("0.0.0.0", UCAST_LISTEN_PORT)])
    return configuration

configuration = {
    "ping_timeout" : 30000,
    "host_timeout" : 500000,
    "ping_interval" : 200000,
    "ping_window" : 100,
    "send_window" : 1000000,
    "recv_window" : 1000000,
    "ring_timeout" : 50000,
    "ring_retry_interval" : 500000,
    "ring_broadcast_interval" : 500000,
    "acceptor_max_pending_instances" : 200000,
    "acceptor_instance_window_size" : 1000000,
    "batch_phase1_timeout" : 300000,
    "phase1_batch_size" : 1000,
    "instance_pool_open_limit" : 160000, # 10 sec worth of 1 Gbit
    "instance_pool_reserved_limit" : 2000, # arbitrary, must tune
    "phase1_timeout" : 100000,
    "phase1_interval" : 640, # much more expensive that phase 2
    "phase2_timeout" : 500000,
    "phase2_interval" : 64, # 15625 * 8000 bytes = 1 Gbit/s
    "recovery_grace_period" : 1500000,
    "recovery_local_metric" : 1,
    "recovery_remote_metric" : 10,
    "recovery_queue_poll_interval" : 500000,
    "recovery_reconnect_delay" : 1000000,
    "recovery_socket_timeout" : 2000000,
    "recovery_retry_delay" : 750000,
    "commit_flush_interval" : 500000,
    "initial_backoff" : 10000,
    "max_backoff" : 2000000,
    "mcast_group" : "239.3.0.1" + ":" + str(MCAST_LISTEN_PORT),
    "master_value_port" : "30000",
    "value_buffer_size" : 30000
}

def main(argv):
    global configuration
    if len(argv) != 2:
        print "usage: gen_config metaconfig.json"
    configuration["hosts"] = genHostConfiguration(loadHosts(argv[1]))
    print json.dumps(configuration, indent=1)

if __name__=='__main__':
    main(sys.argv)
