# MBalloonYARN

In this project, we enhance Hadoop-YARN with a dynamic and adaptive memory management module. Its mainly functions include:

(1) Alleviate OutOfMemory errors for JVM based framework(e.g., Spark and Hadoop).

(2) Adaptive sizing memory allocation to its realtime needs for each task container to improve cluster utilization

For more information, please refer our paper (Under submission)

## Install and compile
For compile, plesse refer `BUILDING.txt` for detail. Since my codebase is built on `Hadoop-2.7.3`, it depends on libprotoc-2.5.0(higher version may report error).

## Docker image
Please use /sequenceiq/hadoop-docker as the docker image for running task. We have tested `/sequenceiq/hadoop-docker:2.4.0`, and it can
both support Hadoop Mapreduce and Spark applications. For understanding how YARN works woth docke, please refer this:
https://hadoop.apache.org/docs/r2.7.2/hadoop-yarn/hadoop-yarn-site/DockerContainerExecutor.html

I have hacked NodeManager, so you do not need to configure mapreduce.map.env, mapreduce.reduce.env, yarn.app.mapreduce.am.env
to indicate docker images when you launch applications. Here are two configurations you need to do: 

In yarn-site.xml
```
<property>
    <name>yarn.nodemanager.container-executor.class</name>
    <value>org.apache.hadoop.yarn.server.nodemanager.DockerContainerExecutor</value>
</property>
````
````
<property>
    <name>yarn.nodemanager.docker-container-executor.exec-name</name>
    <value>/usr/bin/docker(path to your docker)</value>
</property>
````






