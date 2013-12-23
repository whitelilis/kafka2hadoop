package cn.cublog.drunkedcat.kafka2hadoop;


import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShuffleReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    // to be override, not change by default
    public String filter(String raw){
        return raw;
    }
    // to be override, use first word by default
    public String messageString2key(String raw){
        return raw.split("\\s+", -1)[0];
    }
    // to be override
    public boolean validateCheck(String raw){
        return true;
    }

    private List<String> mReplicaBrokers = new ArrayList<String>();

    private String findNewLeader(String anOldLeader, String aTopic, int aPartition, int aPort) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(mReplicaBrokers, aPort, aTopic, aPartition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (anOldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    private PartitionMetadata findLeader(List<String> seedBrokers, int aPort, String aTopic, int aPartition) {
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
        if (returnMetaData != null) {
            mReplicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                mReplicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            String line     = values.next().toString().trim();
            String[] parts  = line.split("\\s+");
            String aTopic   = parts[0].trim();
            int aPartition  = Integer.parseInt(parts[1].trim(), 10);
            long readOffset = Long.parseLong(parts[2].trim(), 10);
            long end        = Long.parseLong(parts[3].trim(), 10);
            long maxReads   = end - readOffset;
            String broker   = parts[4].trim();
            int aPort       = Integer.parseInt(parts[5].trim(), 10);

            List<String> seedBrokers = new ArrayList<String>();
            seedBrokers.add(broker);

            System.out.println("try to reduce " + aTopic + ":" + aPartition + ":" + readOffset + ":" + end);

            PartitionMetadata metadata = findLeader(seedBrokers, aPort, aTopic, aPartition);
            if (metadata == null) {
                System.out.println("Can't find metadata for Topic and Partition. Exiting");
                return;
            }
            if (metadata.leader() == null) {
                System.out.println("Can't find Leader for Topic and Partition. Exiting");
                return;
                //continue;
            }
            String leadBroker = metadata.leader().host();
            String clientName = "Client_" + aTopic + "_" + aPartition;

            System.out.println(leadBroker + ":" + aPort + ":" + clientName);
            SimpleConsumer consumer = new SimpleConsumer(leadBroker, aPort, 100000, 64 * 1024, clientName);


            int numErrors = 0;
            while (maxReads > 0) {
                if (consumer == null) {
                    consumer = new SimpleConsumer(leadBroker, aPort, 100000, 64 * 1024, clientName);
                }
                FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(aTopic, aPartition, readOffset, 100000)
                    .build();
                FetchResponse fetchResponse = consumer.fetch(req);

                if (fetchResponse.hasError()) {
                    numErrors++;
                    // Something went wrong!
                    short code = fetchResponse.errorCode(aTopic, aPartition);
                    System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                    if (numErrors > 5) break;
                    if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                        // We asked for an invalid offset. For simple case ask for the last element to reset
                        //readOffset = getLastOffset(consumer,aTopic, aPartition, kafka.api.OffsetRequest.LatestTime(), clientName);
                        System.err.println("get wrong offset " + readOffset);
                        continue;
                    }
                    consumer.close();
                    consumer = null;
                    try{
                        leadBroker = findNewLeader(leadBroker, aTopic, aPartition, aPort);
                    }catch(Exception e){
                        System.err.println(e);
                    }
                    continue;
                }
                numErrors = 0;

                long numRead = 0;
                for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(aTopic, aPartition)) {
                    if(maxReads < 0){
                        break;
                    }
                    long currentOffset = messageAndOffset.offset();
                    if (currentOffset < readOffset) {
                        System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                        continue;
                    }
                    readOffset = messageAndOffset.nextOffset();
                    ByteBuffer payload = messageAndOffset.message().payload();

                    byte[] bytes = new byte[payload.limit()];
                    payload.get(bytes);

                    //System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
                    String offsetStr = String.valueOf(messageAndOffset.offset());
                    String messageStr = new String(bytes, "UTF-8");

                    numRead++;
                    maxReads--;
                    //System.out.println("get what:" + offsetStr + ":" + messageStr);
                    if(validateCheck(messageStr)){
                        Text seqKey  = new Text(messageString2key(messageStr));
                        Text seqValue = new Text(filter(messageStr));

                        //System.out.println("try to collect " + seqKey + ":" + seqValue);
                        output.collect(seqKey, seqValue);
                    }else{
                        System.err.println("ERROR recored invalidate :" + messageStr);
                        continue;
                    }
                }
                if (numRead == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                    }
                }
            }
            if (consumer != null) consumer.close();
        }
    }
}
