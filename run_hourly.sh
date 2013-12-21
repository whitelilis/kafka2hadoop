#!/bin/bash

#begin java and config
export JAVA_HOME=/usr/lib/jvm/jdk1.6.0_45
export HADOOP_HOME=/opt/hadoop
export CLASSPATH='.'

jars_dir=/home/liuzhe/use_jars
jar_name=shuffle.jar
java=$JAVA_HOME/bin/java
hadoop=$HADOOP_HOME/bin/hadoop


package_str=cn.cublog.drunkedcat.kafka2hadoop

broker=10.10.1.25
port=9092
latest_offset_file_name=last
output_base_name=data_from_kafka
offset_dir_name=offsets

# hdfs tree:
# $topic_base_dir
#   \-- $offset_dir_name
#       \--- offset_name
#       \--- ...
#       \--- last
#   \-- $output_base_name
#       \--- data_with_data
#       \--- ......
#       \--- data_with_data

for f in $jars_dir/*
do
        export CLASSPATH=$f:$CLASSPATH
done

function run_topic(){
        if [ $# -lt 2 ]
        then
                echo "usage run_topic <topic> <hdfs_base_dir> <task_number>"
                exit 1
        fi
        topic=$1
        base_dir=$2
        task_number=$3
        # generate offset range
        $java ${package_str}.InputGenerator $base_dir/$offset_dir_name $broker $port $topic $task_number
        if [ $? -ne 0 ]
        then
            echo "Generate input error"
            exit 1
        fi

        output_path_part=$(date +%y%m%d/%H)
        input_path=$base_dir/$offset_dir_name/$latest_offset_file_name
        output_path=$base_dir/$output_base_name/$output_path_part
        # dangerous!!!
        #$hadoop dfs -rmr $output_path
        $hadoop jar $jars_dir/$jar_name ${package_str}.Shuffle $input_path $output_path $task_number >> ./${topic}_log 2>&1 &
}

run_topic pview  /user/dmp/track_v   70
run_topic pclick /user/dmp/track_c   20
