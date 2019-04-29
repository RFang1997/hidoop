# Additional information

The file system consists of a daemon (DataNode) that must be run on each machine. An HdfsClient class is used to manipulate files in HDFS. Sockets in TCP mode are used to implement the communication
between HdfsClient and DataNode. Each interaction between HdfsClient and DataNode will then be initiated by sending a message specifying the command to be executed by DataNode.
The NameNode allows you to access different information about the DataNodes, in particular the different file fragments distributed on the machines.
