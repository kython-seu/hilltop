package kason.custom.partition.producer.singlethread;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by zhangkai12 on 2017/12/20.
 */
public class CustomPartitionProducer {
    private static final String topic = "demo";
    public static void main(String[] args) {

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        //序列化类型
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("partitioner.class","kason.custom.partition.CustomPartitioner");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(prop);
        int mesNo = 0;

        for (mesNo = 0; mesNo < 100; mesNo++){
            String key = mesNo + "";

            String value = "value " + mesNo;

            kafkaProducer.send(new ProducerRecord<String, String>(topic, key, value));
        }

        kafkaProducer.close();
    }
}
