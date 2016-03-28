#!/bin/bash

# Finds a process corresponding to Granules/Neptune resource and kills it.

# First use JPS to locate the Java process.
r_pid=`jps | grep 'ManagedResource' | cut -d' ' -f1`

# If JPS fails, use ps and grep
if [ -z ${r_pid} ]; then 
    r_pid=`ps -ef | grep 'java' | grep ${USER} | grep 'granules' | cut -d' ' -f2`
fi

if [ -z ${r_pid} ]; then
    echo 'No Resource process found!'
else
    `kill -9 ${r_pid}`
    echo 'Killed the Resource process! PID: '${r_pid}
fi
