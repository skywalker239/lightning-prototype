#!/bin/bash

HOSTS=`cat hosts.txt`

for h in $HOSTS; do
    echo $h
    ssh $h $@
done


