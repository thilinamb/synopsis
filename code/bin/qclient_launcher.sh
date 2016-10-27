#!/usr/bin/env bash

rm /tmp/*.cstat

if [ -e /tmp/clients.out ]
then
    rm /tmp/clients.out
    echo 'Removed log file'
fi

# usage: sh qclient_launcher.sh <number_of_clients(jvms)>  <start_port>  <number_of_threads> <number_of_reqs_per_t>
client_count=${1}
config_file=${pwd}'../config/ResourceConfig.txt'
echo 'Launching '${1}' clients on '${HOSTNAME}

for i in `seq 1 ${client_count}`;
   do
       new_port=$((${2} + i))
       sh client ${config_file} ${new_port} 'query' ${3} ${4} >> /tmp/clients.out &
   done

tail -f /tmp/clients.out
