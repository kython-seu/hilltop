package kason.custom.partition.producer.multithread;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Created by zhangkai12 on 2017/12/21.
 */
public class ProducerRunnable implements Runnable{

    private String topic;
    private KafkaProducer<String, String> kafkaProducer;

    public ProducerRunnable(String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
    }

    @Override
    public void run() {

    }
}
