#!/bin/bash

r_pid=`jps | grep 'ManagedResource' | cut -d' ' -f1`
echo 'Resource PID: '${r_pid}
if [ -z ${r_pid} ]; then 
    r_pid=`ps -ef | grep 'java' | grep 'thilinab' | grep 'granules' | cut -d' ' -f2`
    echo '[PS] Resource PID: '${r_pid}
fi

`kill -9 ${r_pid}`
