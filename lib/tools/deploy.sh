#!/bin/bash

HOSTS=`cat hosts.txt`

for h in $HOSTS; do
    echo $h
    ssh $h mkdir deploy 2>/dev/null
    scp -r $@ skywalker@${h}:deploy
done


