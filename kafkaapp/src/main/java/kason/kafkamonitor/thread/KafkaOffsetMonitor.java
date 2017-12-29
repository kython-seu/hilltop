package kason.kafkamonitor.thread;

import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadata;
import kafka.common.Topic;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.coordinator.GroupMetadataManager;
import kafka.coordinator.GroupTopicPartition;
import kafka.coordinator.OffsetKey;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kason.kafkamonitor.constants.KafkaProperties;
import kason.kafkamonitor.constants.KafkaZKConfig;
import kason.kafkamonitor.entity.KeyAndValueSchemasInfo;
import kason.kafkamonitor.entity.MessageValueStructAndVersionInfo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static kason.kafkamonitor.constants.KafkaProperties.Kafka.CONSUMER_OFFSET_TOPIC;

/**
 * Created by zhangkai12 on 2017/12/28.
 */
public class KafkaOffsetMonitor implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(KafkaOffsetMonitor.class);

    /** Multi cluster information. 区分不同集群, 进而可以同时监控多个集群的kafka状况*/
    public static Map<String, Map<GroupTopicPartition, OffsetAndMetadata>> multiKafkaConsumerOffsets = new ConcurrentHashMap<>();

    private static Map<String, KafkaConsumer> kafkaConsumerMap = new ConcurrentHashMap<>();


    private static Schema OFFSET_COMMIT_KEY_SCHEMA_V0 = new Schema(new Field("group", Type.STRING), new Field("topic", Type.STRING), new Field("partition", Type.INT32));
    private static Field KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("group");
    private static Field KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("topic");
    private static Field KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("partition");

    private static Schema OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", Type.INT64), new Field("metadata", Type.STRING, "Associated metadata.", ""),
            new Field("timestamp", Type.INT64));

    private static Schema OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", Type.INT64), new Field("metadata", Type.STRING, "Associated metadata.", ""),
            new Field("commit_timestamp", Type.INT64), new Field("expire_timestamp", Type.INT64));

    private static Field VALUE_OFFSET_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset");
    private static Field VALUE_METADATA_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata");
    private static Field VALUE_TIMESTAMP_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp");

    private static Field VALUE_OFFSET_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset");
    private static Field VALUE_METADATA_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata");
    private static Field VALUE_COMMIT_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp");


    /** Kafka offset memory in schema. */
    @SuppressWarnings("serial")
    private static Map<Integer, KeyAndValueSchemasInfo> OFFSET_SCHEMAS = new HashMap<Integer, KeyAndValueSchemasInfo>() {
        {
            KeyAndValueSchemasInfo ks0 = new KeyAndValueSchemasInfo();
            ks0.setKeySchema(OFFSET_COMMIT_KEY_SCHEMA_V0);
            ks0.setValueSchema(OFFSET_COMMIT_VALUE_SCHEMA_V0);
            put(0, ks0);

            KeyAndValueSchemasInfo ks1 = new KeyAndValueSchemasInfo();
            ks1.setKeySchema(OFFSET_COMMIT_KEY_SCHEMA_V0);
            ks1.setValueSchema(OFFSET_COMMIT_VALUE_SCHEMA_V1);
            put(1, ks1);
        }
    };
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
        /*Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.KAFKA_BROKERS);
        props.put("group.id", "kafka_offset_monitor_10");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        startOffsetListener(KafkaZKConfig.CLUSTER_ZK_NAME, kafkaConsumer);
        kafkaConsumerMap.put(KafkaZKConfig.CLUSTER_ZK_NAME, kafkaConsumer);*/


        /**
         * 0.10版本以下
         */
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig(KafkaZKConfig.ZOOKEEPER_IP_PORT, "kafkaoffset"));
        startOffsetListener(KafkaZKConfig.CLUSTER_ZK_NAME,consumerConnector);
    }

    private ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }
    /** Listening offset thread method. */
    private static synchronized void startOffsetListener(String clusterAlias, ConsumerConnector consumerConnector) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(CONSUMER_OFFSET_TOPIC, new Integer(1));
        KafkaStream<byte[], byte[]> offsetMsgStream = consumerConnector.createMessageStreams(topicCountMap).get(CONSUMER_OFFSET_TOPIC).get(0);

        ConsumerIterator<byte[], byte[]> it = offsetMsgStream.iterator();
        while (true) {
            MessageAndMetadata<byte[], byte[]> offsetMsg = it.next();
            if (ByteBuffer.wrap(offsetMsg.key()).getShort() < 2) {
                try {
                    GroupTopicPartition commitKey = readMessageKey(ByteBuffer.wrap(offsetMsg.key()));
                    if (offsetMsg.message() == null) {
                        continue;
                    }
                    OffsetAndMetadata commitValue = readMessageValue(ByteBuffer.wrap(offsetMsg.message()));
                    if(commitValue != null) {
                        logger.info("data {}, {}", commitKey.group(), commitValue.offset());
                    }
                    if (multiKafkaConsumerOffsets.containsKey(clusterAlias)) {
                        multiKafkaConsumerOffsets.get(clusterAlias).put(commitKey, commitValue);
                    } else {
                        Map<GroupTopicPartition, OffsetAndMetadata> kafkaConsumerOffsets = new ConcurrentHashMap<>();
                        kafkaConsumerOffsets.put(commitKey, commitValue);
                        multiKafkaConsumerOffsets.put(clusterAlias, kafkaConsumerOffsets);
                    }
                } catch (Exception e) {
                    logger.info("no coming info");
                    e.printStackTrace();
                }
            }
        }
    }

    /** Analysis of Kafka data in topic in buffer. */
    private static GroupTopicPartition readMessageKey(ByteBuffer buffer) {
        short version = buffer.getShort();
        Schema keySchema = schemaFor(version).getKeySchema();
        Struct key = (Struct) keySchema.read(buffer);
        String group = key.getString(KEY_GROUP_FIELD);
        String topic = key.getString(KEY_TOPIC_FIELD);
        int partition = key.getInt(KEY_PARTITION_FIELD);
        return new GroupTopicPartition(group, new TopicPartition(topic, partition));
    }

    /** Analysis of buffer data in metadata in Kafka. */
    private static OffsetAndMetadata readMessageValue(ByteBuffer buffer) {
        MessageValueStructAndVersionInfo structAndVersion = readMessageValueStruct(buffer);
        if (structAndVersion.getValue() == null) {
            return null;
        } else {
            if (structAndVersion.getVersion() == 0) {
                long offset = structAndVersion.getValue().getLong(VALUE_OFFSET_FIELD_V0);
                String metadata = structAndVersion.getValue().getString(VALUE_METADATA_FIELD_V0);
                long timestamp = structAndVersion.getValue().getLong(VALUE_TIMESTAMP_FIELD_V0);
                return new OffsetAndMetadata(new OffsetMetadata(offset, metadata), timestamp, timestamp);
            } else if (structAndVersion.getVersion() == 1) {
                long offset = structAndVersion.getValue().getLong(VALUE_OFFSET_FIELD_V1);
                String metadata = structAndVersion.getValue().getString(VALUE_METADATA_FIELD_V1);
                long commitTimestamp = structAndVersion.getValue().getLong(VALUE_COMMIT_TIMESTAMP_FIELD_V1);
                return new OffsetAndMetadata(new OffsetMetadata(offset, metadata), commitTimestamp, commitTimestamp);
            } else {
                throw new IllegalStateException("Unknown offset message version: " + structAndVersion.getVersion());
            }
        }
    }

    /** Analysis of struct data structure in metadata in Kafka. */
    private static MessageValueStructAndVersionInfo readMessageValueStruct(ByteBuffer buffer) {
        MessageValueStructAndVersionInfo mvs = new MessageValueStructAndVersionInfo();
        if (buffer == null) {
            mvs.setValue(null);
            mvs.setVersion(Short.valueOf("-1"));
        } else {
            short version = buffer.getShort();
            Schema valueSchema = schemaFor(version).getValueSchema();
            Struct value = (Struct) valueSchema.read(buffer);
            mvs.setValue(value);
            mvs.setVersion(version);
        }
        return mvs;
    }

    /** Get instance K&V schema. */
    private static KeyAndValueSchemasInfo schemaFor(int version) {
        return OFFSET_SCHEMAS.get(version);
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
