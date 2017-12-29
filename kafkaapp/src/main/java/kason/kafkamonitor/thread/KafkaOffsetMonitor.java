package kason.kafkamonitor.thread;

import kafka.common.OffsetAndMetadata;
import kafka.common.Topic;
import kafka.coordinator.GroupMetadataManager;
import kafka.coordinator.GroupTopicPartition;
import kafka.coordinator.OffsetKey;
import kason.kafkamonitor.constants.KafkaProperties;
import kason.kafkamonitor.constants.KafkaZKConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhangkai12 on 2017/12/28.
 */
public class KafkaOffsetMonitor implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(KafkaOffsetMonitor.class);

    /** Multi cluster information. 区分不同集群, 进而可以同时监控多个集群的kafka状况*/
    public static Map<String, Map<GroupTopicPartition, OffsetAndMetadata>> multiKafkaConsumerOffsets = new ConcurrentHashMap<>();

    private static Map<String, KafkaConsumer> kafkaConsumerMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {

    }
    static {
        KafkaOffsetMonitor kafkaOffsetMonitor = new KafkaOffsetMonitor();
        new Thread(kafkaOffsetMonitor).start();
    }
    @Override
    public void run() {
        /**
         * 0.10版本
         */
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.KAFKA_BROKERS);
        props.put("group.id", "kafka_offset_monitor_10");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        startOffsetListener(KafkaZKConfig.CLUSTER_ZK_NAME, kafkaConsumer);
        kafkaConsumerMap.put(KafkaZKConfig.CLUSTER_ZK_NAME, kafkaConsumer);
    }


    /** Listening offset thread method with sasl. */
    private static synchronized void startOffsetListener(String clusterAlias, KafkaConsumer<String, String> consumer) {
        logger.info("offset topic name {}", Topic.GroupMetadataTopicName());
        consumer.subscribe(Arrays.asList(Topic.GroupMetadataTopicName()));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Integer.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    if (record != null && record.value() != null) {
                        Object offsetKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key().getBytes()));
                        if (offsetKey instanceof OffsetKey) {
                            GroupTopicPartition commitKey = ((OffsetKey) offsetKey).key();
                            if (commitKey.topicPartition().topic().equals(Topic.GroupMetadataTopicName())) {
                                continue;
                            }
                            logger.info("topic {}", commitKey.topicPartition().topic());
                            OffsetAndMetadata commitValue = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(record.value().getBytes()));
                            long offset = commitValue.offset();
                            logger.info("get offset {}", offset);
                            if (multiKafkaConsumerOffsets.containsKey(clusterAlias)) {
                                multiKafkaConsumerOffsets.get(clusterAlias).put(commitKey, commitValue);
                            } else {
                                Map<GroupTopicPartition, OffsetAndMetadata> kafkaConsumerOffsets = new ConcurrentHashMap<>();
                                kafkaConsumerOffsets.put(commitKey, commitValue);
                                multiKafkaConsumerOffsets.put(clusterAlias, kafkaConsumerOffsets);
                            }
                        } else {
                            logger.info("Consumer group[" + offsetKey.toString() + "] thread has shutdown.");
                        }
                    }
                }
            }
        }catch (WakeupException e){

        }catch (Exception e){
            logger.error("error ", e);
        } finally{
            consumer.close();
        }
    }

    private static synchronized void shutdown(String clusterAlias){
        KafkaConsumer kafkaConsumer = kafkaConsumerMap.get(clusterAlias);
        kafkaConsumer.wakeup();
    }
}
