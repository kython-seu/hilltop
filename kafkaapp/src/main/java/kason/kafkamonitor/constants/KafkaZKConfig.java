package kason.kafkamonitor.constants;

/**
 * Created by zhangkai12 on 2017/12/23.
 */
public class KafkaZKConfig {

    public static final String KAFKA_ZK_BROKER = "/brokers/ids";
    public static final String KAFKA_ZK_BROKER_TOPICS = "/brokers/topics";

    public static final String TOPIC_ISR = "/brokers/topics/%s/partitions/%s/state";
    //zookeeper ip port

    //public static final String ZOOKEEPER_IP_PORT = "hdh6:2181,hdh7:2181,hdh8:2181";
    //public static final String ZOOKEEPER_IP_PORT = "localhost:2181";
    public static final String ZOOKEEPER_IP_PORT = "10.66.71.121:2181,10.66.71.122:2181,10.66.71.123:2181";

    public static final String CLUSTER_ZK_NAME = "cluster_zk";
}
