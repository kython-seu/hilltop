package kason.custom.partition.consumer.function1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhangkai12 on 2017/12/20.
 */
public class ConsumerLoop implements Runnable {

    private final int id;

    private final List<String> topics;

    private final KafkaConsumer<String, String> kafkaConsumer;
    public ConsumerLoop(int id, List<String> topics, String consume_group) {
        this.id = id;
        this.topics = topics;
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.66.71.166:9092");
        props.put("group.id", consume_group);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.kafkaConsumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        try {
            kafkaConsumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Integer.MAX_VALUE);

                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
                    System.out.println(this.id + ": " + data);
                }

            }
        }catch (WakeupException e){

        }finally {
            kafkaConsumer.close();
        }
    }

    public void shutdown() {
        kafkaConsumer.wakeup();
    }
}
