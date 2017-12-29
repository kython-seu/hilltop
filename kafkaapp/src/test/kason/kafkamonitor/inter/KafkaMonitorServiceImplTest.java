package kason.kafkamonitor.inter;

import kason.kafkamonitor.constants.KafkaProperties;
import kason.kafkamonitor.entity.KafkaBrokerInfo;
import kason.kafkamonitor.entity.MetadataInfo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by zhangkai12 on 2017/12/25.
 */
public class KafkaMonitorServiceImplTest {
    private final static Logger logger = LoggerFactory.getLogger(KafkaMonitorServiceImplTest.class);
    @Test
    public void getBrokerInfoList() throws Exception {
        KafkaMonitorServiceImpl kafkaMonitorService = new KafkaMonitorServiceImpl();
        List<KafkaBrokerInfo> brokerInfoList = kafkaMonitorService.getBrokerInfoList();

        for(KafkaBrokerInfo kafkaBrokerInfo: brokerInfoList){
            logger.info("data {}",kafkaBrokerInfo);
        }


    }

    @Test
    public void getConsumer() {
        String consumer = new KafkaMonitorServiceImpl().getConsumer(KafkaProperties.KAFKA_BROKERS);
        logger.info("get consumer {}" , consumer);
    }

    @Test
    public void findTopicLeader() {
        List<MetadataInfo> bayonet_vehiclepass = new KafkaMonitorServiceImpl().findKafkaLeader("BAYONET_VEHICLEPASS");
        for(MetadataInfo metadataInfo: bayonet_vehiclepass){
            logger.info("metadatainfo {}", metadataInfo);
        }
    }

    @Test
    public void getKafkaConsumerGroups(){
        int kafkaConsumerGroups = new KafkaMonitorServiceImpl().getKafkaConsumerGroups(KafkaProperties.KAFKA_BROKERS);
        logger.info("consumer number is {}", kafkaConsumerGroups);
    }


}