#!/bin/bash

docker run -d --name dmem -m 4G -v ~/project/memory_test:/memory ubuntu /memory/a.out

##wait for docker start up
sleep 30


##suspaned
echo "suspend"
docker update --cpu-quota 1000 dmem


sleep 50

##balloon again
echo "balloon again"
docker update -m 6G dmem
docker update --cpu-quota -1 dmem


sleep 5

echo "throttle"
docker update --cpu-quota 1000 dmem
docker update --cpuset-cpus  0 dmem

echo "reclaim"
docker update -m 5632M dmem
docker update -m 5632M dmem
docker update -m 5632M dmem
docker update -m 5120M dmem
docker update -m 5120M dmem
docker update -m 5120M dmem
docker update -m 4608M dmem
docker update -m 4608M dmem
docker update -m 4608M dmem
docker update -m 4096M dmem
docker update -m 4096M dmem
docker update -m 4096M dmem
docker update -m 3584M dmem
docker update -m 3584M dmem
docker update -m 3584M dmem
docker update -m 3072M dmem
docker update -m 3072M dmem
docker update -m 3072M dmem
docker update -m 2560M dmem
docker update -m 2560M dmem
docker update -m 2560M dmem
docker update -m 2048M dmem
docker update -m 2048M dmem
docker update -m 2048M dmem
docker update -m 1536M dmem
docker update -m 1536M dmem
docker update -m 1536M dmem
docker update -m 1024M dmem
docker update -m 1024M dmem
docker update -m 1024M dmem
docker update -m 1024M dmem
docker update -m 1024M dmem
docker update -m 1024M dmem
docker update -m 512M dmem
docker update -m 512M dmem
docker update -m 512M dmem
docker update -m 512M dmem
docker update -m 512M dmem
docker update -m 512M dmem
docker update -m 64M dmem
docker update -m 64M dmem
docker update -m 64M dmem
docker update -m 64M dmem
docker update -m 64M dmem
docker update -m 64M dmem

sleep 50
echo "unthrot cpu"
docker update -m 8G dmem
docker update --cpu-quota -1 dmem
docker update --cpuset-cpus 0-31 dmem


sleep 1000
