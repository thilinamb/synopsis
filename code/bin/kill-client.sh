#!/bin/bash

# Finds a process corresponding to Granules/Neptune resource and kills it.

# First use JPS to locate the Java process.
r_pids=`jps | grep 'Main' | cut -d' ' -f1`

# If JPS fails, use ps and grep
#if [[ ${#r_pids[@]} > 0 ]]; then 
#    r_pids=`ps -ef | grep 'java' | grep ${USER} | grep 'Main' | cut -d' ' -f2`
#fi

echo 'Clients: '${r_pids}
for r_pid in ${r_pids}; do

    if [ -z ${r_pid} ]; then
        echo 'No Resource process found!'
    else
        `kill -9 ${r_pid}`
        echo 'Killed the Client process! PID: '${r_pid}
    fi
done

s_pids=`ps -ef | grep ${USER} | grep 'tail -f /tmp/clients.out' | cut -d' ' -f2`
for s_pid in ${s_pids};do 
    echo 'Script PID: '${s_pid}
    `kill -9 ${s_pid}`
    echo 'Killed the script'
done
