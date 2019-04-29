# Hidoop
Project inspired by the framework Hadoop

The goal of this project is to create a platform with a file system adapted to the concurrent processing of large amounts of data, 
as well as an execution core focused on scheduling and task management according to the "divide and conquer" scheme (map-reduce). 
The architecture and the functionalities of this platform is based (in a simplified way) on those of the
Hadoop platform.

This implementation will be composed of two services:
- a HiDFS (Hidoop Distributed File System) service. It is a management system for distributed files in which a file is divided into 
fragments, each fragment being stored on one of the nodes of the cluster.
- a Hidoop service controlling the distributed and parallel execution of map processing, the recovery of results and the execution of the 
reduce

The different folders are organized as follows :
- data : folder containing the file to be processed
- sh : the different .sh files allowing to run the application
- src : the source code of the application

Additional information is provided in the various files.

