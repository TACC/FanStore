# FanStore
FanStore is a transient shared object store designed for disitrbuted deep learning workloads. FanStore stores datasets in across local storage (e.g., RAM, RAM disk, SSD), and use interconnect for remote file access. FanStore maintains a global name space across nodes and exposes a POSIX-compliant access interface as a regular shared file system. FanStore runs in user space, enabled by the funtion interception technique, which is 10x faster than FUSE. 

Emperically, FanStore has enbabled deep learning training with millions of small files (KB--MB), with a total size of ~100GB on hundreds of compute nodes. 

Technically, FanStore partitions dataset into chunks and stores one or multiple chunks on each node. Metadata of the dataset is replicated across nodes for highly current access. File data is either accessed locally through PCIE or remotely through round-trip MPI messages. 

![alt text](https://github.com/TACC/fanstore/docs/figures/architecture.png "FanStore Architecture")
