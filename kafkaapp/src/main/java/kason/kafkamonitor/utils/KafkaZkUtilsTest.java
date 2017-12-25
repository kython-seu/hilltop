package kason.kafkamonitor.utils;

import kason.kafkamonitor.constants.KafkaZKConfig;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;
import kafka.utils.ZKStringSerializer$;
/**
 * Created by zhangkai12 on 2017/12/23.
 */
public class KafkaZkUtilsTest {
    ZkClient zkClient = null;
    @Before
    public void setUp() throws Exception {
        int zkSessionTimeoutMs = 5000;
        int zkConnectionTimeoutMs = 5000;
        zkClient = new ZkClient("hdh6:2181,hdh7:2181,hdh8:2181",zkSessionTimeoutMs,zkConnectionTimeoutMs,ZKStringSerializer$.MODULE$);
    }

    @Test
    public void getChildrenParentMayNotExist() throws Exception {
        ZooKeeper zk = new ZooKeeper("hdh6:2181,hdh7:2181,hdh8:2181", 10000, null);
        List<String> ids = zk.getChildren(KafkaZKConfig.KAFKA_ZK_BROKER, false);
        for (String id : ids) {
            String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
            System.out.println(id + ": " + brokerInfo);
        }
    }

    @Test
    public void getAllBrokersInCluster() throws Exception {
        KafkaZkUtils.getAllBrokersInCluster();
    }

}