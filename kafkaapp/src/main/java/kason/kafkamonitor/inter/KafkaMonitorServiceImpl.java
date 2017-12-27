package kason.kafkamonitor.inter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kafka.admin.AdminClient;
import kafka.coordinator.GroupOverview;
import kafka.utils.ZkUtils;
import kason.kafkamonitor.constants.KafkaProperties;
import kason.kafkamonitor.constants.KafkaZKConfig;
import kason.kafkamonitor.entity.KafkaBrokerInfo;
import kason.kafkamonitor.entity.MetadataInfo;
import kason.kafkamonitor.impl.KafkaMonitorService;
import kason.kafkamonitor.utils.KafkaZkUtils;
import kason.kafkamonitor.utils.ZKPoolUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Node;

import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;

import kafka.admin.AdminClient.ConsumerGroupSummary;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static kason.kafkamonitor.constants.KafkaZKConfig.CLUSTER_ZK_NAME;
import static kason.kafkamonitor.constants.KafkaZKConfig.TOPIC_ISR;

/**
 * Created by zhangkai12 on 2017/12/23.
 */
public class KafkaMonitorServiceImpl implements KafkaMonitorService {

    private final static Logger logger = LoggerFactory.getLogger(KafkaMonitorServiceImpl.class);
    private ZKPoolUtils zkPool = ZKPoolUtils.getInstance();
    @Override
    public List<KafkaBrokerInfo> getBrokerInfoList() {
        return KafkaZkUtils.getAllBrokersInCluster();
    }

    @Override
    public List<MetadataInfo> findKafkaLeader(String topic) {

        ZkClient zkc = zkPool.getZkClientSerializer(KafkaZKConfig.CLUSTER_ZK_NAME);
        List<MetadataInfo> targets = new ArrayList<>();
        if (ZkUtils.apply(zkc, false).pathExists(KafkaZKConfig.KAFKA_ZK_BROKER_TOPICS)) {
            Seq<String> subBrokerTopicsPaths = ZkUtils.apply(zkc, false).getChildren(KafkaZKConfig.KAFKA_ZK_BROKER_TOPICS);
            List<String> topics = JavaConversions.seqAsJavaList(subBrokerTopicsPaths);
            if (topics.contains(topic)) {
                Tuple2<Option<String>, Stat> tuple = ZkUtils.apply(zkc, false).readDataMaybeNull(KafkaZKConfig.KAFKA_ZK_BROKER_TOPICS + "/" + topic);
                JSONObject partitionObject = JSON.parseObject(tuple._1.get()).getJSONObject("partitions");
                for (String partition : partitionObject.keySet()) {
                    String path = String.format(TOPIC_ISR, topic, Integer.valueOf(partition));
                    Tuple2<Option<String>, Stat> tuple2 = ZkUtils.apply(zkc, false).readDataMaybeNull(path);
                    JSONObject topicMetadata = JSON.parseObject(tuple2._1.get());
                    MetadataInfo metadate = new MetadataInfo();
                    metadate.setIsr(topicMetadata.getString("isr"));
                    metadate.setLeader(topicMetadata.getInteger("leader"));
                    metadate.setPartitionId(Integer.valueOf(partition));
                    metadate.setReplicas(getReplicasIsr(KafkaZKConfig.CLUSTER_ZK_NAME, topic, Integer.valueOf(partition)));
                    targets.add(metadate);
                }
            }
        }
        if (zkc != null) {
            zkPool.releaseZKSerializer(KafkaZKConfig.CLUSTER_ZK_NAME, zkc);
            zkc = null;
        }
        return targets;
    }
    /**
     * According to topic and partition to obtain Replicas & Isr.
     *
     * @param topic
     * @param partitionid
     * @return String.
     */
    private String getReplicasIsr(String clusterAlias, String topic, int partitionid) {
        ZkClient zkc = zkPool.getZkClientSerializer(clusterAlias);
        Seq<Object> repclicasAndPartition = ZkUtils.apply(zkc, false).getInSyncReplicasForPartition(topic, partitionid);
        List<Object> targets = JavaConversions.seqAsJavaList(repclicasAndPartition);
        if (zkc != null) {
            zkPool.releaseZKSerializer(clusterAlias, zkc);
            zkc = null;
        }
        return targets.toString();
    }

    //支持0.10.x, 0.9版本不起作用
    /**
     *
     * @param kafka_brokers
     * @return
     * node PC-ZHANGKAI12.hikvision.com:9092
    14:55:46,614  INFO - size 2
    14:55:46,615  INFO - groupid group_test
    14:55:46,638  INFO - groupid avro_test
    get consumer [{"node":"PC-ZHANGKAI12.hikvision.com:9092","meta":[{"owner":"consumer-1-067bc44c-ad17-402e-8431-8f8d5113779d","node":"10.17.130.29","topicSub":[{"partition":0,"topic":"demo"},{"partition":1,"topic":"demo"},{"partition":2,"topic":"demo"}]}],"group":"group_test"},
    {"node":"PC-ZHANGKAI12.hikvision.com:9092","meta":[{"owner":"consumer-1-aa402521-0735-4a3a-bd93-15085c873ed5","node":"10.17.130.29","topicSub":[{"partition":0,"topic":"avro"},{"partition":1,"topic":"avro"},{"partition":2,"topic":"avro"},{"partition":3,"topic":"avro"}]}],"group":"avro_test"}]
     */
    @Override
    public String getConsumer(String kafka_brokers) {
        JSONArray consumerGroups = new JSONArray();
        try {
            Properties props = new Properties();
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka_brokers);
            AdminClient adminClient = AdminClient.create(props);
            scala.collection.immutable.Map<Node, scala.collection.immutable.List<GroupOverview>> opts = adminClient.listAllConsumerGroups();
            Iterator<Tuple2<Node, scala.collection.immutable.List<GroupOverview>>> groupOverview = opts.iterator();
            while (groupOverview.hasNext()) {
                Tuple2<Node, scala.collection.immutable.List<GroupOverview>> tuple = groupOverview.next();
                String node = tuple._1.host() + ":" + tuple._1.port();
                scala.collection.immutable.List<GroupOverview> groupOverviewList = tuple._2;
                logger.info("size {}", groupOverviewList.length());
                Iterator<GroupOverview> groups = tuple._2.iterator();
                while (groups.hasNext()) {
                    GroupOverview group = groups.next();
                    JSONObject consumerGroup = new JSONObject();
                    String groupId = group.groupId();
                    logger.info("groupid {}", groupId);
                    if (!groupId.contains("kafka.eagle")) {
                        consumerGroup.put("group", groupId);
                        consumerGroup.put("node", node);
                        //consumerGroup.put("meta", getKafkaMetadata(parseBrokerServer(clusterAlias), groupId));// 通过getAllBrokers进行得到broker 信息, 这里直接使用
                        consumerGroup.put("meta", getKafkaMetadata(kafka_brokers, groupId));
                        consumerGroups.add(consumerGroup);
                    }
                }
            }
            adminClient.close();
        } catch (Exception e) {
            logger.error("Get kafka consumer has error,msg is " + e.getMessage());
        }

        return consumerGroups.toJSONString();
    }


    private JSONArray getKafkaMetadata(String bootstrapServers, String group) {
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        /*if (SystemConfigUtils.getBooleanProperty("kafka.eagle.sasl.enable")) {
            sasl(prop, bootstrapServers);
        }*/
        JSONArray consumerGroups = new JSONArray();
        try {
            AdminClient adminClient = AdminClient.create(prop);
            ConsumerGroupSummary cgs = adminClient.describeConsumerGroup(group);
            Option<scala.collection.immutable.List<AdminClient.ConsumerSummary>> opts = cgs.consumers();
            Iterator<AdminClient.ConsumerSummary> consumerSummarys = opts.get().iterator();
            while (consumerSummarys.hasNext()) {
                AdminClient.ConsumerSummary consumerSummary = consumerSummarys.next();
                Iterator<TopicPartition> topics = consumerSummary.assignment().iterator();
                JSONObject topicSub = new JSONObject();
                JSONArray topicSubs = new JSONArray();
                while (topics.hasNext()) {
                    JSONObject object = new JSONObject();
                    TopicPartition topic = topics.next();
                    object.put("topic", topic.topic());
                    object.put("partition", topic.partition());
                    topicSubs.add(object);
                }
                topicSub.put("owner", consumerSummary.consumerId());
                topicSub.put("node", consumerSummary.host().replaceAll("/", ""));
                topicSub.put("topicSub", topicSubs);
                consumerGroups.add(topicSub);
            }
            adminClient.close();
        } catch (Exception e) {
            logger.error("Get kafka consumer metadata has error, msg is " + e.getMessage());
        }
        return consumerGroups;
    }

}
