package kason.custom.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created by zhangkai12 on 2017/12/20.
 */
public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println("topic is " + topic +" key is " + key +"value " + value +" size: " + cluster.partitionCountForTopic(topic));
        //int num =  ((String)key).length() % (cluster.partitionCountForTopic(topic));
        int num =  Integer.parseInt((String)key) % (cluster.partitionCountForTopic(topic));
        System.out.println("partitioner " + num);

        return num;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
