#!/bin/bash


dockerid=`docker inspect --format={{.Id}} container_1506460233144_0061_01_000002`
memory="/sys/fs/cgroup/memory/docker/$dockerid/memory.usage_in_bytes"


time=1200

count=0

echo $memory
echo `date`
dstat -d >> disk.txt&

while [ $count -le $time ]
do

musage=`cat $memory`

echo "$count  $musage" >> mem.txt
sleep 1
count=`expr $count + 1`

done
