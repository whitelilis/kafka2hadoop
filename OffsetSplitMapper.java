package cn.cublog.drunkedcat.kafka2hadoop;


import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class OffsetSplitMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public List<Long> longSplit(long begin, long end, long partition_count){
        List<Long> result = new ArrayList<Long>();
        if(begin >= end){//empty range
            result.add(new Long(end));
            result.add(new Long(end));
        }else{
            result.add(new Long(begin));
            long delta = (end - begin - 1) / partition_count + 1;
            long tmp = begin + delta;
            while(tmp < end){
                result.add(new Long(tmp));
                tmp += delta;
            }
            result.add(new Long(end));
        }
        return result;
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line         = value.toString();
        String[] parts      = line.split("\\s+");
        String topic        = parts[0];
        String partition_id = parts[1];
        long numbers        = Long.parseLong(parts[2].trim(), 10);
        long begin          = Long.parseLong(parts[3].trim(), 10);
        long end            = Long.parseLong(parts[4].trim(), 10);

        List<Long> stones = longSplit(begin, end, numbers);

        for(int i = 0; i < stones.size() - 1; ++i){
            output.collect(new Text("" + i), new Text(topic + " " + partition_id + " " + stones.get(i) + " " + stones.get(i+1)));
        }
    }
}
