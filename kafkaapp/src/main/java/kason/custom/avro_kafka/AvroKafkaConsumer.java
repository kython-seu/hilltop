package kason.custom.avro_kafka;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Created by zhangkai12 on 2017/12/22.
 */
public class AvroKafkaConsumer {

    private KafkaConsumer<String, byte[]> kafkaConsumer;
    private final static Logger logger = LoggerFactory.getLogger(AvroKafkaConsumer.class);
    private List<String> topics;
    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"Iteblog\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\" },"
            + "  { \"name\":\"str2\", \"type\":\"string\" },"
            + "  { \"name\":\"int1\", \"type\":\"int\" }"
            + "]}";
    public AvroKafkaConsumer(List<String> topics, String consume_group) {
        this.topics = topics;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", consume_group);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        this.kafkaConsumer = new KafkaConsumer<>(props);
    }

    public void testConsumer() {
        kafkaConsumer.subscribe(topics);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(ReadSchema.read());
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        try {
            while (true) {
                ConsumerRecords<String, byte[]> poll = kafkaConsumer.poll(Integer.MAX_VALUE);
                for(ConsumerRecord<String, byte[]> record : poll){

                    String key = record.key();
                    byte[] value = record.value();
                    GenericRecord genericRecord = recordInjection.invert(value).get();

                    logger.info("key=" + key + ", str1= " + genericRecord.get("str1")
                            + ", str2= " + genericRecord.get("str2")
                            + ", int1=" + genericRecord.get("int1"));

                }
            }
        }catch (WakeupException e){

        }finally {
            kafkaConsumer.close();
        }

    }

    public void shutdown(){
        kafkaConsumer.wakeup();
    }
}
