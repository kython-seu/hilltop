package kason.kafkamonitor.impl;

import kason.kafkamonitor.entity.KafkaBrokerInfo;

import java.util.List;

/**
 * Created by zhangkai12 on 2017/12/23.
 */
public interface KafkaMonitorService {

    public List<KafkaBrokerInfo> getBrokerInfoList();
    public String getConsumer(String kafka_brokers);
}
