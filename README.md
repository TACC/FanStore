# FanStore
FanStore is a transient shared object store designed for disitrbuted deep learning workloads. FanStore stores datasets in across local storage (e.g., RAM, RAM disk, SSD), and use interconnect for remote file access. FanStore maintains a global name space across nodes and exposes a POSIX-compliant access interface as a regular shared file system. FanStore runs in user space, enabled by the funtion interception technique, which is 10x faster than FUSE. FanStore supports compression, which can significantly increase the storage hardware capacity for some applications. FanStore dramatically reduce the I/O traffic between compute nodes and the shared file system.

Emperically, FanStore has enbabled deep learning training with millions of small files (KB--MB), with a total size of ~100GB on hundreds of compute nodes with close-to-perfect scaling performanace. The following figure shows the ResNet-50 performance scalability of a GPU cluster (4 GPUs per node) an a CPU cluster with Keras, TensorFlow, and Horovod. 

<img src="https://github.com/TACC/FanStore/blob/master/docs/figures/ResNet-50.png" width="320">
<!--- ![alt text](https://github.com/TACC/FanStore/blob/master/docs/figures/ResNet-50.png =320x240 "ResNet-50 performance on a GPU and CPU cluster") --->

Technically, FanStore partitions dataset into chunks and stores one or multiple chunks on each node. Metadata of the dataset is replicated across nodes for highly current access. File data is either accessed locally through PCIE or remotely through round-trip MPI messages, as shown in the following figure. 

![alt text](https://github.com/TACC/FanStore/blob/master/docs/figures/architecture.png "FanStore Architecture")


## Installation
