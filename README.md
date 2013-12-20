kafka2hadoop
============

Dump data from kafka to hadoop


## TODO
- [] read broker from input
- [] set reduce number from inputGenrator file, or make inputGenerator and mapreduce job single step
- [] remove third party jar content out, read from hdfs is the best solution
- [] use direct partitioner
- [] when broker down, do nothing, don't change hdfs
- [] reduce read broker and port from map output
- [] remore core-site.xml hdfs-site.xml absolute path from InputGenerator.java
- [] move kafka related function to new Class KafkaHelper.java
- [] make job name changable, by -D?
- [] move filter function to Shuffle.java
- [] make it easier to use
- [] give reduce offset details, besides string
