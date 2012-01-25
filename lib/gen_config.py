#!/usr/bin/python

import json
import sys

MCAST_SRC_PORT = 23339
MCAST_LISTEN_PORT = 23338
MCAST_REPLY_PORT = 23337
RING_PORT = 23336

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
                              addPort(hostname, RING_PORT)])
    return configuration

configuration = {
    "ping_timeout" : 300000,
    "host_timeout" : 1200000,
    "ping_interval" : 1000000,
    "ping_window" : 100,
    "ring_timeout" : 2000000,
    "ring_retry_interval" : 3000000,
    "mcast_group" : "239.3.0.1" + ":" + str(MCAST_LISTEN_PORT)
}

def main(argv):
    global configuration
    if len(argv) != 2:
        print "usage: gen_config metaconfig.json"
    configuration["hosts"] = genHostConfiguration(loadHosts(argv[1]))
    print json.dumps(configuration, indent=1)

if __name__=='__main__':
    main(sys.argv)