#!/bin/bash
mkdir $1/savime
sudo mount -t tmpfs -o size=$2G,nr_inodes=1000m,mode=777 tmpfs $1/savime
