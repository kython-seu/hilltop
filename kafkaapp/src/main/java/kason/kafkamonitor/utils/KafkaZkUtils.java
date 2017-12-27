package kason.kafkamonitor.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kafka.utils.ZKStringSerializer$;
import kason.kafkamonitor.constants.KafkaZKConfig;
import kason.kafkamonitor.entity.KafkaBrokerInfo;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.utils.ZkUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by zhangkai12 on 2017/12/23.
 */
public class KafkaZkUtils {
    private static final Logger logger = LoggerFactory.getLogger(KafkaZkUtils.class);
    private static ZkClient zkClient;
    static {
        int zkSessionTimeoutMs = 5000;
        int zkConnectionTimeoutMs = 5000;
        zkClient = new ZkClient(KafkaZKConfig.ZOOKEEPER_IP_PORT,zkSessionTimeoutMs,zkConnectionTimeoutMs, ZKStringSerializer$.MODULE$);
    }
    public static List<String> getChildrenParentMayNotExist(String path) {
        try {
            return zkClient.getChildren(path);
        } catch (ZkNoNodeException e) {
            return null;
        } catch (Exception ex) {
            logger.error("getChildrenParentMayNotExist invoke fail!", ex);
            return null;
        }
    }

    public static Pair<String, Stat> readData(String path) {
        Stat stat = new Stat();
        String dataStr = zkClient.readData(path, stat);
        System.out.println(dataStr);
        return Pair.of(dataStr, stat);
    }

    public static Pair<String, Stat> readDataMaybeNull(String path) {
        Stat stat = new Stat();
        Pair<String, Stat> dataAndStat = null;
        try {
            logger.debug("[path {}]", path);
            dataAndStat = Pair.of((String) zkClient.readData(path, stat), stat);
        } catch (ZkNoNodeException nkex) {
            return Pair.of(null, stat);
        } catch (Exception ex) {
            //logger.error(ex);
        }
        return dataAndStat;
    }

    public static KafkaBrokerInfo getBrokerInfo(int brokerId) {
        String brokerInfoStr = readDataMaybeNull( KafkaZKConfig.KAFKA_ZK_BROKER + "/" + brokerId).getLeft();

       /* Pair<String, Stat> brokerInfoStr = readData(zkClient, KafkaZKConfig.KAFKA_ZK_BROKER + "/" + brokerId);
        if(brokerInfoStr != null){
            logger.info("not null");
            logger.info(brokerInfoStr.getLeft());
        }else {
            logger.info("null");
        }*/
        KafkaBrokerInfo kafkaBrokerInfo = null;
        if (StringUtils.isNotEmpty(brokerInfoStr)) {
            logger.debug("[broker info {}] ", brokerInfoStr);
            kafkaBrokerInfo = new KafkaBrokerInfo(brokerId,KafkaZKConfig.KAFKA_ZK_BROKER + "/" + brokerId,brokerInfoStr);
            logger.info("[broker message {}]", kafkaBrokerInfo);

        }
        return kafkaBrokerInfo;
    }

    public static List<KafkaBrokerInfo> getAllBrokersInCluster() {
        List<String> brokers = getChildrenParentMayNotExist(KafkaZKConfig.KAFKA_ZK_BROKER);
        List<KafkaBrokerInfo> brokerLists = new ArrayList<>();
        //if(brokers)
        if (brokers != null) {
            Collections.sort(brokers);
        }

        for (String brokerStr : brokers) {
            logger.debug("broker {} ", brokerStr);
            KafkaBrokerInfo brokerInfo = getBrokerInfo(Integer.parseInt(brokerStr));
            if (brokerInfo != null){
                brokerLists.add(brokerInfo);
            }
        }

        return brokerLists;
    }



    /*public static ZkClient getZkClientSerializer(){

    }*/
    public static void getAllTopics(String topic){
        Pair<String, Stat> stringStatPair = readData(KafkaZKConfig.KAFKA_ZK_BROKER_TOPICS + "/" + topic);
        JSONObject partitionObject = JSON.parseObject(stringStatPair.getLeft()).getJSONObject("partitions");
        for (String partition : partitionObject.keySet()) {
            String path = String.format(KafkaZKConfig.TOPIC_ISR, topic, Integer.valueOf(partition));
            logger.info("path {}", path);
            Pair<String, Stat> stringStatPair1 = readDataMaybeNull(path);
            logger.info("topic str {}", stringStatPair1.getLeft());


            //ZkUtils.apply()
            /*Tuple2<Option<String>, Stat> tuple2 = ZkUtils.apply(zkc, false).readDataMaybeNull(path);
            JSONObject topicMetadata = JSON.parseObject(tuple2._1.get());
            MetadataInfo metadate = new MetadataInfo();
            metadate.setIsr(topicMetadata.getString("isr"));
            metadate.setLeader(topicMetadata.getInteger("leader"));
            metadate.setPartitionId(Integer.valueOf(partition));
            metadate.setReplicas(getReplicasIsr(clusterAlias, topic, Integer.valueOf(partition)));
            targets.add(metadate);*/
        }
    }

}
