# How to run the Sketchlet sample
This sample demonstrates an Sketchlet.

## Building Synopsis
### Pre-requisites
1. Apache Maven 3.0.0 or higher
2. Java 1.8

### Instructions
We will use ``$SYNOPSIS_HOME`` to point to the root directory of the cloned source.
1. Change the directory to code ``$SYNOPSIS_HOME/code``
2. Run ``mvn clean install`` to launch the build process
3. Change the directroy to ``$SYNOPSIS_HOME/code/modules/distribution/target``
4. Unzip the binary distribution ``neptune-geospatial-distribution-1.0-SNAPSHOT-bin.zip``

## Run the sample
### Input Datasets
Let's use NOAA data for this sample. It will work with any dataset formatted correctly. We have hosted NOAA data in the lattice cluster. 
Anyone in the ``galileo`` user group should be able to access this data. The data is partitioned on a monthly basis and stored in machines 
from lattice-1 to lattice-12 --- each machine is dedicated for one month. For e.g., January data is stored in lattice-1, February data is stored in lattice-2, etc. On each machine, data is stored in the following path.

``/s/$HOSTNAME/$DRIVE/nobackup/galileo/noaa-data/2010``

The drive information is available on the following document.

https://docs.google.com/spreadsheets/d/1bDVagJClxntt_W2bmiaTCbJIT_iBY_s30R3D9HDcRhA/edit#gid=0

For instance, 2010 February data is available on drive C of lattice-2 at the location - ``/s/lattice-2/c/nobackup/galileo/noaa-data/2010``.

Inside each of these directories, there will be several files with different extensions. We are interested in files with .mblob extension. 
There will be a separate file for each observation cycle. There will be 4 observation cycles per day. The timestamp of the observation cycle is embedded in the file name.

This program is written to ingest a single file, but it can be extended to ingest multiple files.

### How to run the sample
1. Go into the ``bin`` directory of the unzipped distribution
2. Execute ``sh run_class synopsis.samples.sketch.Sketchlet <path_to_input>

