# How to Build
Prequisites: Maven 3, JDK 1.7 or higher  
1. `> cd code/`  
2. `> mvn clean install` - This will create a distribution with the compiled code and the dependencies.  

# How to run
Prerequisites: Apache ZooKeeper

## Updating the configurations
1. After compiling the source, unzip the generated distribution. It should be inside `code/modules/distribution/target`.  
`> cd code/modules/distribution/target`  
`> unzip unzip neptune-geospatial-distribution-1.0-SNAPSHOT-bin.zip`  
*Configuration files are located inside `conf` sub directory. The main configuration file is `ResourceConfig.txt`. Properties explained below is defined in this file unless specified specifically. Only the ports most relevant to getting a working Synopsis cluster is explained here.*

2. Ports used for communication: By default Synopsis uses 7077 for data traffic (`listener-port`) and 9099 (`control-plane-port`) for control traffic.

2. Configure the Zookeeper endpoints using the property `zookeeper-hosts`.  
For instance, if ZooKeeper is running on localhost, port 9191, it should be set as;  
`zookeeper-hosts=localhost:9191`  
If you run a cluster of ZooKeeper servers, then specify the set of Zookeeper endpoints separated by commas. Refer to [this guide](https://zookeeper.apache.org/doc/r3.3.2/zookeeperAdmin.html#sc_zkMulitServerSetup) to setup a Zookeeper cluster. In the past, we used a 3 node Zookeeper cluster.

3. Change the deployer URL and port in the Granules configuration (`deployer-endpoint`). Deployer is launched in the machine from which you'll launch the job (step 10).  
`deployer-endpoint=lattice-96:9099`

4. Dynamic Scaling: Following configurations are related to setting up dynamic scaling.  
`rivulet-enable-dynamic-scaling=true` - Enables and Disables dynamic scaling  
`rivulet-monitor-interval=1000` - Execution interval (in milliseconds) of the monitoring thread which updates the state and triggers the dynamic scaling if necessary

*Other dynamic scaling properties appearing the configuration are related to backlog length related scaling operations. It is not required to change them unless you are working with variable ingestion rates or testing the dynamic scaling functionality. The primary metric used for scaling is memory and it is set 0.45 of the allocated heap.*

5. Fault tolerance: This is disabled by default as it replicates the state hence requires more resources. To enable, set the following property to true.  
`rivulet-enable-fault-tolerance`

6. Configuring Hazelcast: Hazelcast is used to implement the Gossiping subsystem. You need to configure the set of machines participating in gossip protocol by setting an ip prefix.  
`rivulet-hazelcast-interface=129.82.46.*`

7. Deployment Configuration: Synopsis used Granules underneath as the streaming implementation. Entire Synopsis system can be thought of as a stream processing job deployed on top of Granules. There are two types of operators: stream ingesters and stream processors. Stream ingesters are used to inject data into the system. Sketches are maintained at the stream processors. Granules deployer doesn't allow the binding of a particular operator to a certain machine. This is required, especially when the ingesters need to be deployed on machines where the input data is hosted. Deployement configurations are used to provide this bindings that are used during the initial deployment. Please take a look at the sample deployment configuration available inside the configuration (`air_quality_config.json`). There are two operators denoted by their class names. For instance the ingester (`neptune.geospatial.benchmarks.airquality.AirQualityDataIngester`) will be deployed in lattice-95 where the input data files were hosted.

8. Setting up the stat-server: There is a centralized statistics server used to gather system wide metric readings periodically. This is useful to get a cumulative view of the entire cluster over time. Otherwise joining statistics reported locally at individual machines is both error-prone and cumbersome. This is a very lightweight java server which will periodically dump the system metrics along with the timestamp to the file system of the machine where it is running. Update the stat server endpoint using the property `stat-server-endpoint`.

## Starting a Synopsis cluster
1. Start Zookeeper.

2. Start the stat-server.

3. Start the Synopsis nodes (these are basically Granules resources).   
To launch a single node use the following startup script.  
`> cd neptune-geospatial-distribution-1.0-SNAPSHOT/bin`  
`> sh resource -c ../config/ResourceConfig.txt`

If you need to run a cluster with a number of machines, use `dssh` script to launch Granules resources in a set of machines simulatenously. More details on dssh is available [here](https://github.com/malensek/dssh). Following command will launch Granules resource in the list of machines specificed in the file machine_list (list of line separated ip/hostnames. Check the machines file in the conf for an example.).  
`> ./dssh -cap -f <path_to_machine_list> 'cd cd neptune-geospatial-distribution-1.0-SNAPSHOT/bin; sh resource -c ../config/ResourceConfig.txt'`

*Usually allow 1-2 minutes to complete the startup process of the cluster. Some lattice machines are slower than the others.*

### Terminating a cluster  
You can use the `dssh` script for that combined with the `kill-resource.sh` script in the `bin` directory.  
`> ./dssh -cap -f machines 'cd ~/research/rivulet/code/modules/distribution/target/neptune-geospatial-distribution-1.0-SNAPSHOT/b;sh kill-resource.sh'`  

Make sure to restart the stat-server and deployer everytime you restart the cluster. Zookeeper will reset its state in about 20-30s after a node is terminated. So allow about 30s between cluster restarts. This is easier than restarting ZK. (Probably you can write a simple script to delete the znode where node information is stored to avoid this wait.)

## Ingesting data
To launch a job,  
`> cd neptune-geospatial-distribution-1.0-SNAPSHOT/bin`  
`> sh granules-start -c ../config/ResourceConfig.txt -t <class_name_of_the_job>`  
For instance: `> sh granules-start -c ../config/ResourceConfig.txt -t neptune.geospatial.benchmarks.sketch.DynamicScalingGraph`

You should launch this task in the machine designated as the deployer node (step 3 in the "Updating the configuration" section).

## Checkpointing
Checkpointing will dump the in-memory state of the entire cluster to the disk. Restoring will load a previous checkpoint and recreate the in-memory state. This cuts down the data ingestion times for repeating benchmarks.

### Creating a checkpoint
Once the data ingestion is complete, execute the following command from a single node. It will connect the ZooKeeper cluster and identify the active Synopsis nodes and send a serialization request.  
`> ./run_class synopsis.client.Main ../config/ResourceConfig.txt client_port store`  
For instance: `> ./run_class synopsis.client.Main ../config/ResourceConfig.txt 9000 store`

This command will gracefully complete if it has heard back from all the nodes. Otherwise, it will forcefully terminate after 5 minutes. Upone successful completion of this command, it will store some metadata that are required to restore this checkpoint later. It will be stored in the `/tmp` directory with the extension `.pstat`. File name will be the unique identifier assigned by the client to persistence request. For instance, if the final line of the output of this command is as follows;  
`2017-12-10 19:27:36 INFO  PersistenceManager:42 - Persistence task is completed. Received last ack. Task id: 1512959167339`  

Then there should be a file named '1512959167339.pstat' in the `/tmp` directory. 

Save this file for restoring this checkpoint later. This file contains the location of the file created during the checkpoint process at each node. By keeping track of this file location, it is possible to create multiple independent checkpoints.

### Restoring from a checkpoint
For restoring from a checkpoint, we will deploy an empty stream processing graph and populate its state from the persisted checkpoint data.

1. Start the ZK cluster

2. Start the stat server

3. Deploy the empty graph. Run the following command from the node designated as the deployer node.  
`> ./granules-start -c ../config/ResourceConfig.txt -t synopsis.client.persistence.LoadStateFromDiskDeployer path_to_pstat_file`  
For instance: `> ./granules-start -c ../config/ResourceConfig.txt -t synopsis.client.persistence.LoadStateFromDiskDeployer ~/1512959167339.pstat`
