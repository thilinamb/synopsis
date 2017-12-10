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

2. Start Zookeeper and update the ZooKeeper URL property (`"zookeeper-hosts"`) in the Granules configuration file (`neptune-geospatial-distribution-1.0-SNAPSHOT/config/config/ResourceConfig.txt`).  
For instance, if ZooKeeper is running on localhost, port 9191, it should be set as;  
`zookeeper-hosts=localhost:9191`  
If you run a cluster of ZooKeeper servers, then specify the set of Zookeeper endpoints separated by commas.  

3. Change the deployer URL and port in the Granules configuration (`"deployer-endpoint"`). Deployer is launched in the machine from which you'll launch the job (step 4).
`deployer-endpoint=lattice-96:9099`

4. Dynamic Scaling: Following configurations are related to setting up dynamic scaling.
`rivulet-enable-dynamic-scaling=true` - Enables and Disables dynamic scaling
rivulet-monitor-interval=1000 - Execution interval (in milliseconds) of the monitoring thread which updates the state and triggers the dynamic scaling if necessary

5. Start the Granules resource.  
`> cd neptune-geospatial-distribution-1.0-SNAPSHOT/bin`  
`> sh resource -c ../config/ResourceConfig.txt`  

6. To launch a job,  
`> cd neptune-geospatial-distribution-1.0-SNAPSHOT/bin`  
`> sh granules-start -c ../config/DeployerConfig.txt -t <class_name_of_the_job>`
