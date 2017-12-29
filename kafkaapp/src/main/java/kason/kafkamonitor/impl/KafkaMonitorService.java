package kason.kafkamonitor.impl;

import kason.kafkamonitor.entity.KafkaBrokerInfo;
import kason.kafkamonitor.entity.MetadataInfo;

import java.util.List;

/**
 * Created by zhangkai12 on 2017/12/23.
 */
public interface KafkaMonitorService {

    public List<KafkaBrokerInfo> getBrokerInfoList();

    /**
     * 获取所有的消费者组信息
     * @param kafka_brokers
     * @return String
     */
    public String getConsumer(String kafka_brokers);


    /** Get kafka sasl topic metadate. */
    public List<MetadataInfo> findKafkaLeader(String topic);


    /** Get kafka consumer groups.numbers */
    public int getKafkaConsumerGroups(String kafka_brokers);

    /** Get kafka 0.10.x offset from topic. */
    public String getKafkaOffsetFromAllTopic(String kafka_brokers);
}
