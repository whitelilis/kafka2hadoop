package cn.cublog.drunkedcat.kafka2hadoop;

import cn.cublog.drunkedcat.kafka2hadoop.OffsetRange;

import java.io.*;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

public class InputGenerator{
    static Configuration conf;
    static FileSystem fs;
    final String HADOOP_HOME = System.getenv("HADOOP_HOME");

    public static void help(){
        System.err.println("Usage: java InputGenerator <hdfs_offset_base_dir> <broker> <port> <topic> [split_number]");
    }

    public InputGenerator(){
        conf = new Configuration();
        conf.addResource(new Path(HADOOP_HOME, "conf/core-site.xml"));
        conf.addResource(new Path(HADOOP_HOME, "conf/hdfs-site.xml"));
        try{
            fs = FileSystem.get(conf);
        }catch(Exception e){
            System.out.println(e);
        }
    }


    public static String simDate(){
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        Date date = new Date();
        return dateFormat.format(date);
    }

    // write data to hdfs
    protected void writeToHdfs(String path, List<String> content) throws Exception {
        Path outPath = new Path(path);
        if (fs.exists(outPath)){
            throw new Exception("path " + path + " exist." );
        }
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.create(outPath)));
        for( String s: content){
            bufferedWriter.write(s + "\n");
        }
        bufferedWriter.close();
    }


    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }


    public PartitionMetadata findLeader(List<String> seedBrokers, int aPort, String aTopic, int aPartition) {
        PartitionMetadata returnMetaData = null;
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, aPort, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = new ArrayList<String>();
                topics.add(aTopic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == aPartition) {
                            returnMetaData = part;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + aTopic
                        + ", " + aPartition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        // if (returnMetaData != null) {
        //     m_replicaBrokers.clear();
        //     for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
        //         m_replicaBrokers.add(replica.host());
        //     }
        // }
        return returnMetaData;
    }


    public List<OffsetRange> allOffset(String broker, int port, String topic){
        List<OffsetRange> result = new ArrayList<OffsetRange>();
        List<String> brokerList = new ArrayList<String>();
        brokerList.add(broker);

        int partitionId = 0;
        PartitionMetadata metadata = null;
        do{
            metadata = findLeader(brokerList, port, topic, partitionId);
            if (metadata == null) {
                //System.out.println("Can't find metadata for Topic and Partition. Exiting");
                break;
            }
            if (metadata.leader() == null) {
                System.out.println("Can't find Leader for Topic and Partition. Exiting");
                return null;
            }

            OffsetRange tmp = new OffsetRange();
            tmp.topic = topic;
            tmp.partition = partitionId;
            tmp.leadBroker = metadata.leader().host();

            String clientName = "Client_" + topic + "_" + partitionId;
            //System.out.println(leadBroker + ":" + port + ":" + clientName);
            SimpleConsumer consumer = new SimpleConsumer(tmp.leadBroker, port, 100000, 64 * 1024, clientName);
            tmp.begin = getLastOffset(consumer,topic, partitionId, kafka.api.OffsetRequest.EarliestTime(), clientName);
            tmp.end   = getLastOffset(consumer,topic, partitionId, kafka.api.OffsetRequest.LatestTime(), clientName) + 1;
            result.add(tmp);
            ++partitionId;
        }while(metadata != null);
        return result;
    }


    static boolean nothingTodo(List<OffsetRange> rg){
        boolean result = false;
        if(rg.size() == 0){
            result = true;
        }
        return result;
    }

    public static void main(String[] args){
        // generate input for job run

        if (args.length < 4){
            help();
        }else{
            String hdfsBaseDir  = args[0];
            String hdfsInput    = hdfsBaseDir + "/last"; // where to put input file, if exist, use as last run
            String timeFilePath = hdfsBaseDir + "/" + simDate();
            String broker       = args[1];
            int port            = Integer.valueOf(args[2]);
            String topic        = args[3];
            int splitNumber = 20; // by default, split to 20 tasks
            if (args.length > 4){// user given splitNumber, override default
                splitNumber = Integer.valueOf(args[4]);
            }

            InputGenerator ig = new InputGenerator();
            List<OffsetRange> rg = ig.allOffset(broker, port, topic);
            List<String> content = new ArrayList<String>();

            if (nothingTodo(rg)){// something is error, do nothing and exit
                return;
            }

            Path inputPath = new Path(hdfsInput);
            try{
                if(fs.exists(inputPath)){// start from last run, regenerate offset
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
                    String line;
                    line = br.readLine();
                    while (line != null){
                        line = line.trim();
                        String[] parts = line.split("\\s+");
                        int oldPartition = Integer.parseInt(parts[1].trim(), 10);
                        long oldReadOffset = Long.parseLong(parts[4].trim(), 10);
                        // the belowing for using hash maybe faster
                        for(OffsetRange ir : rg){
                            if(ir.partition == oldPartition && ir.begin < oldReadOffset && oldReadOffset <= ir.end){
                                ir.begin = oldReadOffset;
                            }
                        }
                        line=br.readLine();
                    }
                    fs.delete(inputPath);
                }
                for(OffsetRange i : rg){
                    content.add((topic + " " + i.partition + " " + splitNumber + " " + i.begin + " " + i.end + " " + i.leadBroker + " " + port).trim());
                }
                ig.writeToHdfs(timeFilePath, content);
                ig.writeToHdfs(hdfsInput, content);
            }catch(Exception e){
                System.out.println(e);
            }
        }
    }
}
