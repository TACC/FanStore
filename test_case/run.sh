#!/bin/bash

export FS_ROOT=/dev/shm/fs_`id -u`
export LD_PRELOAD=`pwd`/../wrapper.so

./read_file_mpi ./data/flist /dev/shm/fs_`id -u`
