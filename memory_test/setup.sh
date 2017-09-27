#!/bin/bash

dmem=""

##wait for docker start up
sleep 30


##suspaned
echo "suspend"
docker update --cpu-quota 1000 $dmem
docker update --cpuset-cpus  0 $dmem

echo "reclaim"

for i in `seq 64 -1 1`;
do
   docker update -m ${i}G --memory-swap 100G $dmem
   docker update -m ${i}G --memory-swap 100G $dmem
   docker update -m ${i}G --memory-swap 100G $dmem
done

sleep 600
echo "unthrot cpu"
docker update -m 8G dmem
docker update --cpu-quota -1 dmem
docker update --cpuset-cpus 0-31 dmem


sleep 1000
