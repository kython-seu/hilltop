package kason.custom.message;




import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created by zhangkai12 on 2017/7/31.
 */
public class ProducerPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println("topic is " + topic +" key is " + key +"value " + value +" size: " + cluster.partitionCountForTopic(topic));
        int num =  ((String)key).length() % (cluster.partitionCountForTopic(topic));
        System.out.println("partitioner " + num);

        return num;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
    /*public ProducerPartitioner(){}
    public ProducerPartitioner(VerifiableProperties props){}
    @Override
    public int partition(Object key, int numPartitions) {
        System.out.println("key is " + key +" partitioner " + numPartitions);

        return ((String)key).length() % numPartitions;
    }*/
}
