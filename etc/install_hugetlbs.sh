#!/bin/bash
#See HUGETBLS configs:
cat /proc/meminfo

#Set number of pages to 20. Change 20 according to your needs:
echo 20 > /proc/sys/vm/nr_hugepages

#Mount file system. If the user applications are going to request huge pages using mmap system
#call, then it is required that system administrator mount a file system of type hugetlbfs:
mkdir /mnt/huge
sudo mount -t hugetlbfs none /mnt/huge/
