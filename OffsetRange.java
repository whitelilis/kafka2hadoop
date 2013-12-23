package cn.cublog.drunkedcat.kafka2hadoop;


public class OffsetRange{
    public String topic;
    public int partition;
    public String leadBroker;
    public long begin;
    public long end;
    public void show(){
        System.out.println(topic + ":" + partition + "@" + leadBroker + " " + begin + "===>" + end);
    }
}
