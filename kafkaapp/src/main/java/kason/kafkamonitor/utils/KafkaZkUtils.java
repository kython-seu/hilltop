package kason.kafkamonitor.utils;

import kason.kafkamonitor.constants.KafkaZKConfig;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Created by zhangkai12 on 2017/12/23.
 */
public class KafkaZkUtils {


    private static final Logger logger = LoggerFactory.getLogger(KafkaZkUtils.class);

    public static List<String> getChildrenParentMayNotExist(ZkClient zkClient, String path) {
        try {
            return zkClient.getChildren(path);
        } catch (ZkNoNodeException e) {
            return null;
        } catch (Exception ex) {
            logger.error("getChildrenParentMayNotExist invoke fail!", ex);
            return null;
        }
    }

    public static Pair<String, Stat> readData(ZkClient client, String path) {
        Stat stat = new Stat();
        String dataStr = client.readData(path, stat);
        System.out.println(dataStr);
        return Pair.of(dataStr, stat);
    }

    public static Pair<String, Stat> readDataMaybeNull(ZkClient client, String path) {
        Stat stat = new Stat();
        Pair<String, Stat> dataAndStat = null;
        try {
            logger.info("[path {}]", path);
            dataAndStat = Pair.of((String) client.readData(path, stat), stat);
        } catch (ZkNoNodeException nkex) {
            return Pair.of(null, stat);
        } catch (Exception ex) {
            //logger.error(ex);
        }
        return dataAndStat;
    }

    public static void getBrokerInfo(ZkClient zkClient, int brokerId) {
        String brokerInfoStr = readDataMaybeNull(zkClient, KafkaZKConfig.KAFKA_ZK_BROKER + "/" + brokerId).getLeft();
       /* Pair<String, Stat> brokerInfoStr = readData(zkClient, KafkaZKConfig.KAFKA_ZK_BROKER + "/" + brokerId);
        if(brokerInfoStr != null){
            logger.info("not null");
            logger.info(brokerInfoStr.getLeft());
        }else {
            logger.info("null");
        }*/
        if (StringUtils.isNotEmpty(brokerInfoStr)) {
            logger.info("[broker info {}] ", brokerInfoStr);
        }
    }

    public static void getAllBrokersInCluster(ZkClient zkClient) {
        List<String> brokers = getChildrenParentMayNotExist(zkClient, KafkaZKConfig.KAFKA_ZK_BROKER);
        //if(brokers)
        if (brokers != null) {
            Collections.sort(brokers);
        }

        for (String brokerStr : brokers) {
            logger.info("broker {} ", brokerStr);
            getBrokerInfo(zkClient, Integer.parseInt(brokerStr));
        }

    }


}
