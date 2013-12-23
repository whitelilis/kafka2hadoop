package cn.cublog.drunkedcat.kafka2hadoop;


import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Shuffle extends ShuffleReducer{

    //override reducer filter
    public String filter(String raw){
        return raw;
    }

    public static void main(String[] args) throws Exception {
        final int maxArguIndex = 2; // 3 arguments
        int splitNumber = 120; // can be override by command arguments

        if (args.length > maxArguIndex){// user given splitNumber, override default
            splitNumber = Integer.valueOf(args[maxArguIndex].trim());
        }
        JobConf conf = new JobConf(Shuffle.class);

        conf.setNumReduceTasks(splitNumber);
        conf.setJobName("kafka2hadoop");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(OffsetSplitMapper.class);
        conf.setReducerClass(Shuffle.class);
        conf.setInputFormat(TextInputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
