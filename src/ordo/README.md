# Additional information

The Hidoop service provides support for distributed execution. A demon (Daemon) must be launched on each machine. We use the **RMI API** for communication between this daemon
and its customers. In **Job.java**, the behavior of *startJob* is to run maps (with *runMap*) on all the demons of the machines, then wait until all the maps are finished. The maps generated local files
on machines that are fragments. Once these local files are generated, they are collected and a local reduction is applied to the collected file. We can then retrieve the global file with HDFS, then 
apply a final reduction to the various local reductions recovered.
