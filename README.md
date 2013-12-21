# kafka2hadoop
- This project is aimed at making kafka->hadoop flow simple.
- This is a java project, a hadoop map/reduce job, it loads kafka message to hadoop hdfs.

## Features
- automatically track topic reading offsets on hdfs, use text, every time only read new message, when nothing new, do nothing.
- when something goes wrong, you can edit the offsets information by any editor.
- use map/reduce job to load message, the task number can be controled through map/reduce way.
- you can filter the message by extending or change job class : Shuffle.java

## Content
- a kafka offset manage util : InputGenerator
- a hadoop map/reduce job related file
- a simple bash script we use hourly
- third party jars

## Example
See *run_hourly.sh* for details, we only run it hourly through conttab, nothing more is needed.

## Build From Source
If you want to build it from source, follow the steps below:
- for it is a java project, you need jdk first
- unjar scala-libary.jar kafka_2.8.0-0.8.0-bete1.jar metrics-core.jar here, all of these is need to run kafka2hadoop map/reduce job
- ensure hadoop related jars is in CLASSPATH
- just run 'make', then you will get shuffle.jar
- put shuffle.jar in your CLASSPATH, then you can use InputGenerator, and shuffle.jar is the usable hadoopjar


## TODO
- [X] read broker from input
- [X] set reduce number from inputGenrator file, or make inputGenerator and mapreduce job single step(through bash script)
- [] remove third party jar content out, read from hdfs is the best solution
- [] use direct partitioner
- [X] when broker down, do nothing, don't change hdfs
- [X] reduce read broker and port from map output
- [] remore core-site.xml hdfs-site.xml absolute path from InputGenerator.java
- [] move kafka related function to new Class KafkaHelper.java
- [] make job name changable, by -D?
- [X] move filter function to Shuffle.java
- [X] make it easier to use(run_hourly.sh)
- [] give reduce offset details, besides message only
- [] remove magic numbers
- [] when different broker has different port, InputGenerator won't work
