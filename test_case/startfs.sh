#!/bin/bash

unset LD_PRELOAD
export FS_ROOT=/dev/shm/fs_`id -u`

../read_remote_file 12 data
