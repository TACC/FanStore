# FanStore
FanStore is a transient shared object store designed for disitrbuted deep learning workloads. FanStore stores datasets in across local storage (e.g., RAM, RAM disk, SSD), and use interconnect for remote file access. FanStore maintains a global name space across nodes and exposes a POSIX-compliant access interface as a regular shared file system. FanStore runs in user space, enabled by the funtion interception technique, which is 10x faster than FUSE. FanStore supports compression, which can significantly increase the storage hardware capacity for some applications. FanStore dramatically reduce the I/O traffic between compute nodes and the shared file system.

Emperically, FanStore has enbabled deep learning training with millions of small files (KB--MB), with a total size of ~100GB on hundreds of compute nodes with close-to-perfect scaling performanace. The following figure shows the ResNet-50 performance scalability of a GPU cluster (4 GPUs per node) an a CPU cluster with Keras, TensorFlow, and Horovod. 

<img src="https://github.com/TACC/FanStore/blob/master/docs/figures/ResNet-50.png" width="320">
<!--- ![alt text](https://github.com/TACC/FanStore/blob/master/docs/figures/ResNet-50.png =320x240 "ResNet-50 performance on a GPU and CPU cluster") --->

Technically, FanStore partitions dataset into chunks and stores one or multiple chunks on each node. Metadata of the dataset is replicated across nodes for highly current access. File data is either accessed locally through PCIE or remotely through round-trip MPI messages, as shown in the following figure. 

![alt text](https://github.com/TACC/FanStore/blob/master/docs/figures/architecture.png "FanStore Architecture")

## Limitation
For now, FanStore only surpports x86_64 instruction set and has been tested with Intel Xeon Phi 7250, Intel Xeon Platinum 8160, and Intel Xeon CPUs with NVIDIA V100 and GTX 1080 Ti GPUs.

We are in the progress of porting FanStore to IBM POWER9 platforms.

## Installation

    git clone https://github.com/TACC/FanStore.git
    cd FanStore
    make

## Usage
To user FanStore, there are two steps: data preparation and loading.

### Data Preparation
Assuming you are in the dataset directory, in which there is a training dataset in **_train_** and a validation dataset called **_val_**, first, we need to generate a list of files and directories

    find ./ > file.list

Then we build the dataset using FanStore. The following command line prepares the dataset in such a way: all data in the **_val_** path will be broadcasted to all nodes, while the rest of the files will be scattered.

    /path/to/prep 8 file.list val

Optionnally, you can pass a compression level parameter to the above command, e.g.

    /path/to/prep 8 file.list val pack_10

If you do not have a validation dataset, use **_NULL_** as a place holder. E.g.

     /path/to/prep 8 file.list NULL

After successfuly compeletion of the preparation, you should see a list of file partitions with name of **_fs\_\*_**" and a **_dir.list_** file. These are the prepared datasets.

### Loading Data
Now let's load the prepared dataset to local storage. In this case, we use **_/tmp_**.