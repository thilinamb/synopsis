#!/usr/bin/env bash

# usage: sh qclient_launcher.sh <number_of_clients(jvms)>  <start_port>  <number_of_threads> <number_of_reqs_per_t>
client_count=${1}
config_file=${pwd}'../config/ResourceConfig.txt'
echo 'Launching '${1}' clients on '${HOSTNAME}

for i in `seq 1 ${client_count}`;
   do
       new_port = $((${2} + 1))
       sh client.sh ${config_file} ${new_port} 'query' ${3} ${4}
   done

