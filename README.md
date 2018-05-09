# mElas

In this project, we enhance Hadoop-YARN with a dynamic and adaptive memory management module. Its mainly functions include:

(1) Alleviate OutOfMemory errors for JVM based framework(e.g., Spark and Hadoop).

(2) Adaptive sizing memory allocation to its in-time needs for each task container to improve cluster utilization (So it is called elastic).

For more information, please refer our paper (Currently under submission).

## Install and compile
For building this project, plesse refer `BUILDING.txt` for detail. Since my codebase is built on `Hadoop-2.7.3`, it depends on libprotoc-2.5.0(higher version may report error).

## Docker image
Please use /sequenceiq/hadoop-docker as the docker image for running task. We have tested `/sequenceiq/hadoop-docker:2.4.0`, and it can
both support Hadoop Mapreduce and Spark applications. For understanding how YARN works with docke, please refer this:

https://hadoop.apache.org/docs/r2.7.2/hadoop-yarn/hadoop-yarn-site/DockerContainerExecutor.html

I have hacked NodeManager, so you do not need to configure mapreduce.map.env, mapreduce.reduce.env, yarn.app.mapreduce.am.env
to indicate docker images when you launch mapreduce applications. Here are two configurations you need to do: 

in yarn-site.xml
```
<property>
    <name>yarn.nodemanager.container-executor.class</name>
    <value>org.apache.hadoop.yarn.server.nodemanager.DockercontainerExecutor</value>
</property>
````
````
<property>
    <name>yarn.nodemanager.docker-container-executor.exec-name</name>
    <value>/usr/bin/docker(path to your docker)</value>
</property>
````

## MBalloon Configuration
There are 4 parameters needed to be configured by users, all in yarn-site.xml.
1. Balloon ratio.
```
<property>
    <name>yarn.nodemanager.balloon.ratio</name>
    <value>0.4</value>
</property>

````
2. Stop ballooing limit(SB).
````
<property>
    <name>yarn.nodemanager.balloon.stop</name>
    <value>0.8</value>
</property>
````
3. JVM heap size for Flex containers (MAX_HEAP).
````
<property>
    <name>yarn.nodemanager.balloon.jvm-mb</name>
    <value>65536</value>
</property>
````

4. Flex contaienr size (MIN_CONT). It should be configured at job submission; for MapReduce applications, by setting 
`mapreduce.map.memory.mb` and `mapreduce.reduce.memory.mb`; For Spark applications, by setting `spark.executor.memory`. 

5. Flex containers type for applications should be set at job submission. We achieve this by utilizing node label expression. 
For MapReduce, I have some hardcode to tell YARN to recognise this applications as FLEX. But this issue will be
resolved in future Hadoop Release(e.g., Hadoop-2.8.0/Hadoop3.0.0) For Spark:
````
spark.yarn.am.nodeLabelExpression          flex 
````

This project is still under active development, if you have any questions, feel free to contact ynjassionchen@gmail.com

