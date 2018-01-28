#!/bin/bash



dmem="container_1506460233144_0061_01_000002"

##wait for docker start up
sleep 30


##suspaned
echo "suspend"
docker update --cpu-quota 1000 $dmem
docker update --cpuset-cpus  0 $dmem

echo "reclaim"

for i in `seq 65 -1 4`;
do
   docker update -m ${i}G --memory-swap 100G $dmem
   docker update -m ${i}G --memory-swap 100G $dmem
   docker update -m ${i}G --memory-swap 100G $dmem
done

echo "sleep"
sleep 300

echo "unthrot cpu"
docker update -m 70G --memory-swap 100G $dmem
docker update --cpu-quota -1 $dmem
docker update --cpuset-cpus 0-31 $dmem
echo "finish"

