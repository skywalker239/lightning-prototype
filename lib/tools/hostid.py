#!/usr/bin/python
from __future__ import with_statement
import os

import simplejson as json
import socket


def main():
    with file(os.path.expanduser('~/deploy/metaconfig.json'), 'r') as f:
        hostnames = [x[0] for x in json.load(f)]
        hostname = socket.gethostbyaddr(socket.gethostname())[0]
        print hostnames.index(hostname)

if __name__=='__main__':
    main()

