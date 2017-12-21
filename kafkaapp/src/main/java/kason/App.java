package kason;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;


public class App {
    public static void main( String[] args ) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.65.223.192:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<String, String>(props);
        //String[] keys = new String[]{"aa","bb","cc","dd","ee","ff","gg","hh","ii","jj","kk","ll"};
        String[] keys = new String[]{"aa","bb","cc","dd"};
        Random random = new Random();
        for(int i = 0; i < 1000; i++){
            int length = random.nextInt(keys.length);
            StringBuilder sb = new StringBuilder("");
            for(int j=0; j< length;j++){
                sb.append(keys[random.nextInt(keys.length)]);
                sb.append(" ");
            }
            producer.send(new ProducerRecord<String, String>("testtopic",sb.toString()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
