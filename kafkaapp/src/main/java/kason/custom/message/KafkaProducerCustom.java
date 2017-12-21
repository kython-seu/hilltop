package kason.custom.message;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;

/**
 * Created by zhangkai12 on 2017/7/31.
 */
public class KafkaProducerCustom {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.65.223.192:9092");
        //props.put("metadata.broker.list","10.65.223.192:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);//16KB
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);//32MB
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "KeyWord");
        //props.put("serializer.class","KeyWordMessage");

        //props.put("partitioner.class","ProducerPartitioner");
        KeyWord keyWord = new KeyWord();
        keyWord.setId(4);
        keyWord.setKeyWord("keyworddddd");
        keyWord.setUser("admin");
        keyWord.setDate(new Date());

        org.apache.kafka.clients.producer.Producer kafkaProducer = new KafkaProducer(props);

        kafkaProducer.send(new ProducerRecord<String,KeyWord>("test","deee",keyWord), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                System.out.println("finished");
            }
        });
        kafkaProducer.close();




    }
}
