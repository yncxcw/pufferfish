#!/bin/bash


dockerid=`docker inspect --format={{.Id}} dmem`
memory="/sys/fs/cgroup/memory/docker/$dockerid/memory.usage_in_bytes"


time=1200

count=0

echo $memory

while [ $count -le $time ]
do

musage=`cat $memory`

echo "$count  $musage" >> mem.txt
sleep 1
count=`expr $count + 1`

done
