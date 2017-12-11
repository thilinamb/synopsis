# neptune-geospatial
This project aims at using Neptune to process geo-spatial data and answer approximate queries. 

# How to Build
Prequisites: Maven 3, JDK 1.7 or higher  
1. `> cd code/`  
2. `> mvn clean install` - This will create a distribution with the compiled code and the dependencies.  

# How to run
Prerequisites: Apache ZooKeeper  

1. After compiling the source, unzip the generated distribution. It should be inside `code/modules/distribution/target`.  
`> cd code/modules/distribution/target`  
`> unzip unzip neptune-geospatial-distribution-1.0-SNAPSHOT-bin.zip`  
*Configuration files are located inside `conf` sub directory. The main configuration file is `ResourceConfig.txt`. Properties explained below is defined in this file unless specified specifically.*

2. Ports used for communication: By default Synopsis uses 7077 for data traffic (`listener-port`) and 9099 (`control-plane-port`) for control traffic.

2. Start Zookeeper and update the ZooKeeper URL property (`"zookeeper-hosts"`).  
For instance, if ZooKeeper is running on localhost, port 9191, it should be set as;  
`zookeeper-hosts=localhost:9191`  
If you run a cluster of ZooKeeper servers, then specify the set of Zookeeper endpoints separated by commas.  

3. Change the deployer URL and port in the Granules configuration (`"deployer-endpoint"`). Deployer is launched in the machine from which you'll launch the job (step 10).  
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

8. Setting up the stat-server: There is a centralized statistics server used to gather system wide metric readings periodically. This is useful to get a cumulative view of the entire cluster over time. Otherwise joining statistics reported locally at individual machines is both error-prone and cumbersome. This is a very lightweight java server which will periodically dump the system metrics along with the timestamp to the file system of the machine where it is running.

5. Start the Granules resource.  
`> cd neptune-geospatial-distribution-1.0-SNAPSHOT/bin`  
`> sh resource -c ../config/ResourceConfig.txt`  

6. To launch a job,  
`> cd neptune-geospatial-distribution-1.0-SNAPSHOT/bin`  
`> sh granules-start -c ../config/DeployerConfig.txt -t <class_name_of_the_job>`
